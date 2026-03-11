from keep_alive import keep_alive
import discord
from discord.ext import commands
from discord import app_commands
import io
import asyncio
import random
import os
import aiohttp
from dotenv import load_dotenv

load_dotenv()

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix='!', intents=intents, help_command=None)

# Global State
games = {}
VALID_WORDS = set()

# ==========================================
# GAME DATABASES
# ==========================================
THEMES = ["Cyberpunk 🦾", "Horror 🧛‍♂️", "Wild West 🤠", "Office Drama 📎", "Medieval Fantasy 🐉", "Romantic Comedy 💕"]
CORPSE_PROMPTS = [
    "1️⃣ Type an **Adjective** (e.g., creepy, shiny, grumpy):",
    "2️⃣ Type a **Noun** (e.g., alien, toaster, cowboy):",
    "3️⃣ Type a **Verb in past tense** (e.g., hugged, destroyed, ate):",
    "4️⃣ Type another **Adjective** (e.g., depressed, radioactive):",
    "5️⃣ Type another **Noun** (e.g., refrigerator, ghost):",
    "6️⃣ Type a **Location** (e.g., in a McDonald's bathroom, on Mars):"
]
SPYFALL_LOCATIONS = [
    "Airplane ✈️", "Bank 🏦", "Beach 🏖️", "Casino 🎰", "Cathedral ⛪", 
    "Corporate Party 👔", "Crusader Army 🛡️", "Day Spa 💆", "Embassy 🌍", 
    "Hospital 🏥", "Hotel 🏨", "Military Base 🪖", "Movie Studio 🎬", 
    "Ocean Liner 🛳️", "Passenger Train 🚂", "Pirate Ship 🏴‍☠️", "Polar Station 🥶", 
    "Police Station 🚓", "Restaurant 🍽️", "School 🏫", "Space Station 🚀", 
    "Submarine 🌊", "Supermarket 🛒", "Theater 🎭", "University 🎓"
]
BOMB_SYLLABLES = ["TH", "ER", "IN", "ON", "AT", "CH", "ST", "RE", "QU", "BL", "CK", "ING", "OU", "SH", "TR", "PL"]


# ==========================================
# 1. UI GENERATORS & UTILS
# ==========================================
def get_lobby_embed(guild_id):
    game = games[guild_id]
    players = game['players']
    gt = game.get('game_type', 'none')
    host = game['host']
    
    titles = {
        'none': ("🎮 Bot Menu", "Select a mini-game from the dropdown below!", discord.Color.dark_theme()),
        'telephone': ("🎙️ Broken Telephone", "Voice mimicry game! (3+ players)", discord.Color.blue()),
        'corpse': ("📝 Exquisite Corpse", "Absurd collaborative story! (3+ players)", discord.Color.purple()),
        'spyfall': ("🕵️ Spyfall", "Find the spy among you! (3+ players)", discord.Color.dark_gray()),
        'bomb': ("💣 Word Bomb", "Battle Royale typing game! (2+ players)", discord.Color.red())
    }
    
    title, desc, color = titles[gt]
    embed = discord.Embed(title=title, description=desc, color=color)
    embed.set_footer(text=f"Hosted by {host.display_name} • Use /help for rules")
    
    if gt != 'none':
        if not players:
            embed.add_field(name="Players Lobby", value="*No players yet. Click Join!*", inline=False)
        else:
            players_list = "\n".join([f"**{i+1}.** 🎮 {p.display_name}" for i, p in enumerate(players)])
            embed.add_field(name=f"Players Lobby ({len(players)})", value=players_list, inline=False)
    return embed

async def cleanup_game(guild_id):
    if guild_id in games:
        game = games[guild_id]
        if game.get('timeout_task') and not game['timeout_task'].done():
            game['timeout_task'].cancel()
        game['active'] = False
        game['lobby_open'] = False


# ==========================================
# 2. SPYFALL DYNAMIC DASHBOARD 
# ==========================================
class SpyfallTargetSelect(discord.ui.Select):
    def __init__(self, players, current_player, guild_id):
        options = [discord.SelectOption(label=p.display_name, value=str(p.id)) for p in players if p != current_player]
        # Failsafe if options is somehow empty
        if not options: options = [discord.SelectOption(label="Error", value="error")]
        
        super().__init__(placeholder=f"Select your target, {current_player.display_name}...", min_values=1, max_values=1, options=options, custom_id=f"spy_target_{guild_id}")

    async def callback(self, interaction: discord.Interaction):
        game = games.get(interaction.guild.id)
        if not game or not game['active']: return
        
        current_player = game['players'][game['current_player_index']]
        if interaction.user != current_player:
            return await interaction.response.send_message("❌ It's not your turn!", ephemeral=True)
            
        target_id = int(self.values[0])
        target_player = discord.utils.get(game['players'], id=target_id)
        
        # Pass the turn
        game['current_player_index'] = game['players'].index(target_player)
        
        # 1. Update the Dashboard silently
        embed = discord.Embed(title="🕵️ Spyfall: Interrogation Phase", color=discord.Color.dark_gray())
        embed.add_field(name="Current Turn:", value=f"👉 It is **{target_player.mention}**'s turn to pick a target.", inline=False)
        await interaction.response.edit_message(embed=embed, view=SpyfallDashboard(interaction.guild.id))
        
        # 2. Send the highly visible ping in the channel
        await interaction.channel.send(f"🗣️ **{target_player.mention}**, you are being interrogated by **{current_player.mention}**!\nAnswer their question, then use the menu above to pick the next target.")

class SpyfallVoteSelect(discord.ui.Select):
    def __init__(self, players, guild_id):
        options = [discord.SelectOption(label=p.display_name, value=str(p.id)) for p in players]
        super().__init__(placeholder="Vote for the Spy...", min_values=1, max_values=1, options=options, custom_id=f"spy_vote_{guild_id}")

    async def callback(self, interaction: discord.Interaction):
        game = games.get(interaction.guild.id)
        if not game or not game['active']: return
        
        if interaction.user not in game['players']:
            return await interaction.response.send_message("You are not playing!", ephemeral=True)
            
        if interaction.user.id in game['votes']:
            return await interaction.response.send_message("You already voted!", ephemeral=True)
            
        target_id = int(self.values[0])
        game['votes'][interaction.user.id] = target_id
        
        majority_needed = len(game['players']) // 2 + 1
        vote_counts = {}
        for v in game['votes'].values():
            vote_counts[v] = vote_counts.get(v, 0) + 1
            
        await interaction.response.send_message(f"🗳️ **{interaction.user.display_name}** locked in their vote.", delete_after=5.0)
        
        for pid, count in vote_counts.items():
            if count >= majority_needed:
                accused = discord.utils.get(game['players'], id=pid)
                is_spy = (accused == game['spy'])
                
                embed = discord.Embed(title="⚖️ The Village has spoken!", color=discord.Color.red())
                embed.add_field(name="Executed:", value=accused.mention, inline=False)
                if is_spy:
                    embed.add_field(name="Result:", value="🎉 **VILLAGE WINS!** You caught the Spy!", inline=False)
                else:
                    embed.add_field(name="Result:", value=f"💀 **SPY WINS!** You executed an innocent!\n\nThe real Spy was {game['spy'].mention}.\nThe location was **{game['location']}**.", inline=False)
                
                await interaction.channel.send(embed=embed)
                await cleanup_game(interaction.guild.id)
                return

class SpyfallDashboard(discord.ui.View):
    def __init__(self, guild_id):
        super().__init__(timeout=None)
        self.guild_id = guild_id
        game = games[guild_id]
        current_player = game['players'][game['current_player_index']]
        self.add_item(SpyfallTargetSelect(game['players'], current_player, guild_id))

    @discord.ui.button(label="Call Vote ⚖️", style=discord.ButtonStyle.danger, row=1, custom_id="spy_vote_btn")
    async def vote_callback(self, interaction: discord.Interaction, button: discord.ui.Button):
        await trigger_spyfall_vote(interaction)

async def trigger_spyfall_vote(interaction: discord.Interaction):
    guild_id = interaction.guild.id
    game = games.get(guild_id)
    if not game or game.get('game_type') != 'spyfall' or not game.get('active'): 
        return await interaction.response.send_message("❌ No active Spyfall game to vote on.", ephemeral=True)
    if interaction.user not in game['players']: 
        return await interaction.response.send_message("❌ You are not playing!", ephemeral=True)
    
    game['votes'] = {} 
    view = discord.ui.View()
    view.add_item(SpyfallVoteSelect(game['players'], guild_id))
    
    embed = discord.Embed(title="🚨 EMERGENCY MEETING", description=f"{interaction.user.mention} called a vote!\nSelect who you think the spy is. Majority rules.", color=discord.Color.red())
    await interaction.response.send_message(embed=embed, view=view)


# ==========================================
# 3. GAME TIMERS & LOGIC
# ==========================================
async def finish_telephone(guild_id, guess_text):
    game = games.get(guild_id)
    if not game or not game['active']: return
    try: await game['players'][-1].send("✅ Received! Returning to the server...")
    except Exception: pass

    file_first = discord.File(io.BytesIO(game['first_audio']), filename="1_Original.ogg")
    file_last = discord.File(io.BytesIO(game['final_audio']), filename="2_Final_Mimic.ogg")

    embed = discord.Embed(title="🏁 The Broken Telephone has finished!", color=discord.Color.green())
    safe_guess = guess_text[:1000] + "..." if len(guess_text) > 1000 else guess_text
    embed.add_field(name="🧠 The Final Guess", value=f"**{safe_guess}**", inline=False)
    await game['channel'].send(embed=embed, files=[file_first, file_last])
    await cleanup_game(guild_id)

async def finish_corpse(guild_id):
    game = games.get(guild_id)
    if not game or not game['active']: return

    ans = game['corpse_answers']
    final_sentence = f"The **{ans[0]}** **{ans[1]}** **{ans[2]}** the **{ans[3]}** **{ans[4]}** — **{ans[5]}**."

    embed = discord.Embed(title="📝 Exquisite Corpse: The Masterpiece!", color=discord.Color.purple())
    embed.add_field(name=f"Theme: {game['theme']}", value=final_sentence, inline=False)
    await game['channel'].send(embed=embed)
    await cleanup_game(guild_id)

async def next_bomb_turn(guild_id, game):
    if len(game['players']) == 1:
        winner = game['players'][0]
        embed = discord.Embed(title="🏆 BOMB SURVIVOR!", description=f"**{winner.mention} is the last one standing and wins the game!** 🎉", color=discord.Color.gold())
        await game['channel'].send(embed=embed)
        return await cleanup_game(guild_id)
        
    game['current_player_index'] = (game['current_player_index']) % len(game['players'])
    next_player = game['players'][game['current_player_index']]
    game['syllable'] = random.choice(BOMB_SYLLABLES)
    
    await game['channel'].send(f"💣 Passed to {next_player.mention}! (Syllable: **{game['syllable']}**, Time: **{game['time_limit']}s**)")
    game['timeout_task'] = bot.loop.create_task(bomb_timeout(guild_id, next_player))

async def bomb_timeout(guild_id, player):
    game = games.get(guild_id)
    time_limit = game['time_limit']
    
    if time_limit > 5:
        await asyncio.sleep(time_limit - 5)
        if game and game['active'] and game['players'][game['current_player_index']] == player:
            await game['channel'].send(f"⚠️ **5 SECONDS LEFT, {player.mention}!**", delete_after=4.0)
        await asyncio.sleep(5)
    else:
        await asyncio.sleep(time_limit)
    
    if game and game['active'] and game['players'][game['current_player_index']] == player:
        embed = discord.Embed(title="💥 BOOM!", description=f"{player.mention} exploded and is eliminated!", color=discord.Color.dark_red())
        await game['channel'].send(embed=embed)
        
        game['players'].remove(player)
        await next_bomb_turn(guild_id, game)

async def dm_timeout(guild_id, player):
    await asyncio.sleep(60)
    game = games.get(guild_id)
    if game and game['active']:
        is_guesser = (game['game_type'] == 'telephone' and game.get('waiting_for_guess') and game['players'][-1] == player)
        if is_guesser:
            await finish_telephone(guild_id, "*Ran out of time!* ⏰")
        elif game['players'][game['current_player_index']] == player:
            try: await player.send("⏳ Time's up! The game was cancelled.")
            except Exception: pass
            embed = discord.Embed(title="⏰ Time's Up!", description=f"{player.mention} took too long in DMs. Game over!", color=discord.Color.red())
            await game['channel'].send(embed=embed)
            await cleanup_game(guild_id)


# ==========================================
# 4. LOBBY & MENUS (Unified System)
# ==========================================
class GameSelect(discord.ui.Select):
    def __init__(self, guild_id):
        options = [
            discord.SelectOption(label="Broken Telephone", description="Voice mimicry game", emoji="🎙️", value="telephone"),
            discord.SelectOption(label="Exquisite Corpse", description="Absurd collaborative story", emoji="📝", value="corpse"),
            discord.SelectOption(label="Spyfall", description="Find the spy among you", emoji="🕵️", value="spyfall"),
            discord.SelectOption(label="Word Bomb", description="Battle Royale typing game", emoji="💣", value="bomb")
        ]
        super().__init__(placeholder="Host, choose a game...", min_values=1, max_values=1, options=options, custom_id=f"game_select_{guild_id}")

    async def callback(self, interaction: discord.Interaction):
        guild_id = interaction.guild.id
        game = games[guild_id]
        
        if interaction.user != game['host']:
            return await interaction.response.send_message("❌ Only the Host can pick the game!", ephemeral=True)
            
        game['game_type'] = self.values[0]
        gt = game['game_type']
        
        if gt == 'telephone': game.update({'first_audio': None, 'final_audio': None, 'waiting_for_guess': False})
        elif gt == 'corpse': game.update({'corpse_answers': [], 'theme': '', 'corpse_step': 0})
            
        # Refreshes the embed while keeping the entire view intact
        await interaction.response.edit_message(embed=get_lobby_embed(guild_id), view=self.view)

class LobbyView(discord.ui.View):
    def __init__(self, guild_id):
        super().__init__(timeout=None)
        self.guild_id = guild_id
        # The dropdown is now a permanent part of the Lobby UI until started
        self.add_item(GameSelect(guild_id))

    @discord.ui.button(label="Join", style=discord.ButtonStyle.green, custom_id="join_btn", row=1)
    async def join_callback(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user in games[self.guild_id]['players']:
            return await interaction.response.send_message("You are already in!", ephemeral=True)
        games[self.guild_id]['players'].append(interaction.user)
        await interaction.response.edit_message(embed=get_lobby_embed(self.guild_id), view=self)

    @discord.ui.button(label="Leave", style=discord.ButtonStyle.red, custom_id="leave_btn", row=1)
    async def leave_callback(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user not in games[self.guild_id]['players']:
            return await interaction.response.send_message("You are not in the game!", ephemeral=True)
        games[self.guild_id]['players'].remove(interaction.user)
        await interaction.response.edit_message(embed=get_lobby_embed(self.guild_id), view=self)

    @discord.ui.button(label="Start Game", style=discord.ButtonStyle.blurple, custom_id="start_btn", row=1)
    async def start_callback(self, interaction: discord.Interaction, button: discord.ui.Button):
        game = games[self.guild_id]
        
        if interaction.user != game['host']:
            return await interaction.response.send_message("❌ Only the Host can start the game!", ephemeral=True)
            
        if game['game_type'] == 'none':
            return await interaction.response.send_message("❌ Select a game from the dropdown first!", ephemeral=True)
            
        min_p = 2 if game['game_type'] == 'bomb' else 3
        if len(game['players']) < min_p:
            return await interaction.response.send_message(f"Need at least {min_p} players!", ephemeral=True)
        
        random.shuffle(game['players'])
        game['active'] = True
        game['current_player_index'] = 0
        gt = game['game_type']
        
        if gt == 'spyfall':
            game['location'] = random.choice(SPYFALL_LOCATIONS)
            game['spy'] = random.choice(game['players'])
            game['votes'] = {}
            loc_list_str = "\n".join(SPYFALL_LOCATIONS)
            
            for p in game['players']:
                try:
                    if p == game['spy']:
                        await p.send(f"🕵️ **YOU ARE THE SPY!**\nTry to blend in. Here are the locations:\n{loc_list_str}")
                    else:
                        await p.send(f"📍 **Location:** {game['location']}\n🕵️ There is a spy. Ask questions!\n\nLocations:\n{loc_list_str}")
                except Exception:
                    await interaction.channel.send(f"❌ Cannot DM {p.mention}. Game cancelled.")
                    return await cleanup_game(self.guild_id)
            
            first_p = game['players'][0]
            embed = discord.Embed(title="🕵️ Spyfall Started!", color=discord.Color.dark_gray())
            embed.add_field(name="Check your DMs for roles!", value=f"🗣️ **{first_p.mention}**, start the game by selecting someone to interrogate below!", inline=False)
            await interaction.response.edit_message(embed=embed, view=SpyfallDashboard(self.guild_id))
            return 
            
        elif gt == 'bomb':
            game['syllable'] = random.choice(BOMB_SYLLABLES)
            game['used_words'] = set()
            game['time_limit'] = 15.0 
            game['turn_count'] = 0
            first_player = game['players'][0]
            
            embed = discord.Embed(title="💣 BATTLE ROYALE BOMB STARTED!", description="Type a single, real English word containing the syllable to survive.", color=discord.Color.red())
            await interaction.response.edit_message(embed=embed, view=None)
            
            await interaction.channel.send(f"💣 The bomb is ticking! {first_player.mention}, you have 15s! Syllable: **{game['syllable']}**")
            game['timeout_task'] = bot.loop.create_task(bomb_timeout(self.guild_id, first_player))
            return

        shuffled_list = "\n".join([f"**{i+1}.** {p.display_name}" for i, p in enumerate(game['players'])])
        start_embed = discord.Embed(title="🚀 Game Started!", description="Check your DMs.", color=discord.Color.gold())
        start_embed.add_field(name="Turn Order:", value=shuffled_list, inline=False)
        await interaction.response.edit_message(embed=start_embed, view=None)
        
        first_player = game['players'][0]
        try:
            if gt == 'telephone':
                await first_player.send("You are the **FIRST** player! 🎙️\nRecord a VOICE message. You have 60s!")
            elif gt == 'corpse':
                game['theme'] = random.choice(THEMES)
                await first_player.send(f"🎭 **Exquisite Corpse** started!\n🎬 **Theme:** {game['theme']}\n\n**Step 1 of 6**\n{CORPSE_PROMPTS[0]}\n*(60s to reply!)*")
            game['timeout_task'] = bot.loop.create_task(dm_timeout(self.guild_id, first_player))
        except Exception:
            await game['channel'].send(f"❌ Error: Cannot DM {first_player.mention}. Game cancelled.")
            await cleanup_game(self.guild_id)

class ResignViewEnd(discord.ui.View):
    def __init__(self, g_id):
        super().__init__(timeout=None)
        self.guild_id = g_id
    @discord.ui.button(label="Resign 🏳️", style=discord.ButtonStyle.red)
    async def resign_callback(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_message(view=None) 
        await finish_telephone(self.guild_id, "*Resigned / No idea* 🏳️")

# ==========================================
# 5. SLASH COMMANDS & ROUTER
# ==========================================
@bot.tree.command(name="help", description="View the BabbleBox Bot manual and game rules")
async def help_cmd(interaction: discord.Interaction):
    embed = discord.Embed(title="🎮 BabbleBox Bot Manual", description="Gather your friends in a voice or text channel and let the chaos begin!", color=discord.Color.gold())
    embed.add_field(name="🚀 How to Play", value="Type `/play` to open the lobby. The person who types it is the **Host**. The Host chooses the game and clicks Start.", inline=False)
    embed.add_field(name="🎙️ Broken Telephone (3+ Players)", value="The bot DMs the first player to record a voice message. The next player receives it, listens (it self-destructs in 15s!), and mimics it. The final player types a guess of what the original phrase was.", inline=False)
    embed.add_field(name="📝 Exquisite Corpse (3+ Players)", value="A blind story-building game. The bot DMs players asking for an adjective, noun, verb, etc., based on a random theme. Nobody sees the full sentence until the hilarious finale!", inline=False)
    embed.add_field(name="🕵️ Spyfall (3+ Players)", value="The bot DMs everyone a location, except one person who gets 'Spy'. Ask each other questions to find the spy. Use the dropdown to pass the turn, and type `/vote` or click the button to execute someone!", inline=False)
    embed.add_field(name="💣 Word Bomb (2+ Players)", value="A Battle Royale in the server chat. The bot gives a syllable (e.g., 'TH'). You have 15 seconds to type a valid English word containing it. If you fail, you explode. Last one standing wins!", inline=False)
    
    await interaction.response.send_message(embed=embed)

@bot.tree.command(name="play", description="Open the BabbleBox Bot menu and host a game")
async def play_cmd(interaction: discord.Interaction):
    guild_id = interaction.guild.id
    if guild_id in games and games[guild_id].get('lobby_open'):
        return await interaction.response.send_message(embed=discord.Embed(title="❌ Game Room is busy!", description="A lobby or game is already active on this server.", color=discord.Color.red()), ephemeral=True)
        
    games[guild_id] = {
        'host': interaction.user, 'lobby_open': True, 'game_type': 'none', 
        'players': [], 'active': False, 'current_player_index': 0, 
        'channel': interaction.channel, 'timeout_task': None
    }
    await interaction.response.send_message(embed=get_lobby_embed(guild_id), view=LobbyView(guild_id))

@bot.tree.command(name="vote", description="Trigger a Spyfall vote")
async def vote_cmd(interaction: discord.Interaction):
    await trigger_spyfall_vote(interaction)

async def handle_bomb_turn(message, guild_id, game):
    if not game['active']: return # Race condition safety check
    
    if len(message.content.split()) > 1:
        return await message.channel.send("❌ Single words only!", delete_after=3.0)
        
    word = message.content.strip().lower()
    syllable = game['syllable'].lower()
    
    if syllable not in word: return 
    
    if word not in VALID_WORDS:
        return await message.channel.send(f"❌ '{word}' is not a valid English word!", delete_after=3.0)
        
    if word in game['used_words']:
        return await message.channel.send(f"❌ '{word}' was already used!", delete_after=3.0)
    
    if game.get('timeout_task'): game['timeout_task'].cancel()
    game['used_words'].add(word)
    await message.add_reaction("✅")
    
    game['turn_count'] += 1
    if game['turn_count'] % 5 == 0 and game['time_limit'] > 3.0:
        game['time_limit'] -= 1.0 
        await message.channel.send(f"⚠️ **SPEED UP!** Timer is now {game['time_limit']}s!")
    
    game['current_player_index'] += 1
    await next_bomb_turn(guild_id, game)

async def handle_corpse_turn(message, guild_id, game):
    if not message.content or message.attachments: return await message.channel.send("❌ Please send TEXT!")
    if game.get('timeout_task'): game['timeout_task'].cancel()
    
    await message.channel.send("✅ Saved!")
    safe_text = message.content.strip()[:100]
    game['corpse_answers'].append(safe_text)
    game['corpse_step'] += 1
    
    if game['corpse_step'] >= 6:
        await finish_corpse(guild_id)
    else:
        next_idx = game['corpse_step'] % len(game['players'])
        game['current_player_index'] = next_idx
        next_player = game['players'][next_idx]
        try:
            await next_player.send(f"🎬 **Theme:** {game['theme']}\n\n**Step {game['corpse_step'] + 1} of 6**\n{CORPSE_PROMPTS[game['corpse_step']]}\n*(60s to reply!)*")
            game['timeout_task'] = bot.loop.create_task(dm_timeout(guild_id, next_player))
        except Exception:
            await game['channel'].send(f"❌ Cannot DM {next_player.mention}. Game cancelled.")
            await cleanup_game(guild_id)

async def handle_telephone_turn(message, guild_id, game):
    if game.get('waiting_for_guess'):
        if not message.content or message.attachments: return await message.channel.send("❌ Please TYPE your guess in text!")
        if game.get('timeout_task'): game['timeout_task'].cancel()
        await finish_telephone(guild_id, message.content.strip())
    else:
        if not message.attachments: return await message.channel.send("❌ Please send a VOICE message!")
        attachment = message.attachments[0]
        if not (attachment.filename.endswith('.ogg') or (attachment.content_type and 'audio' in attachment.content_type)):
            return await message.channel.send("❌ Send a VOICE message!")

        if game.get('timeout_task'): game['timeout_task'].cancel()
        await message.channel.send("✅ Voice message received!")
        audio_bytes = await attachment.read()
        
        if game['current_player_index'] == 0: game['first_audio'] = audio_bytes
        game['final_audio'] = audio_bytes 
        game['current_player_index'] += 1
        
        if game['current_player_index'] == len(game['players']) - 1:
            game['waiting_for_guess'] = True
            last_player = game['players'][-1]
            try:
                audio_file = discord.File(io.BytesIO(audio_bytes), filename="voice.ogg")
                await last_player.send("You are the **FINAL** player! 🎧\nListen and **TYPE your guess**!\n*(Audio self-destructs in 15s)*", file=audio_file, view=ResignViewEnd(guild_id), delete_after=15.0)
                game['timeout_task'] = bot.loop.create_task(dm_timeout(guild_id, last_player))
            except Exception: await cleanup_game(guild_id)
        else:
            next_player = game['players'][game['current_player_index']]
            try:
                audio_file = discord.File(io.BytesIO(audio_bytes), filename="voice.ogg")
                await next_player.send("Your turn (Mimic)! 🎙️\nRECORD your best mimic!\n*(Audio self-destructs in 15s)*", file=audio_file, delete_after=15.0)
                game['timeout_task'] = bot.loop.create_task(dm_timeout(guild_id, next_player))
            except Exception: await cleanup_game(guild_id)

@bot.event
async def on_message(message):
    if message.author == bot.user: return

    if isinstance(message.channel, discord.DMChannel):
        for guild_id, game in games.items():
            if game.get('active') and game.get('game_type') in ['corpse', 'telephone']:
                if game['players'][game['current_player_index']] == message.author:
                    if game['game_type'] == 'corpse': await handle_corpse_turn(message, guild_id, game)
                    elif game['game_type'] == 'telephone': await handle_telephone_turn(message, guild_id, game)
                    break 

    elif message.guild:
        guild_id = message.guild.id
        game = games.get(guild_id)
        if game and game.get('active') and game.get('game_type') == 'bomb':
            if game['players'][game['current_player_index']] == message.author:
                if message.channel == game['channel']:
                    await handle_bomb_turn(message, guild_id, game)

@bot.event
async def on_ready():
    await bot.tree.sync()
    print("Commands synced globally.")
    
    print("Fetching English dictionary...")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get("https://raw.githubusercontent.com/dwyl/english-words/master/words_alpha.txt") as resp:
                text = await resp.text()
                VALID_WORDS.update(text.splitlines())
        print(f"Dictionary loaded! {len(VALID_WORDS)} words ready for Word Bomb.")
    except Exception as e:
        print(f"Failed to load dictionary: {e}")
    print(f'Bot {bot.user} is fully optimized and ready! 🎈')

if __name__ == '__main__':
    keep_alive()  # Wakes up the Flask web server
    bot.run(os.getenv('DISCORD_TOKEN'))