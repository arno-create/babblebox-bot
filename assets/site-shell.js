(() => {
  const navRoots = document.querySelectorAll("[data-site-nav]");

  navRoots.forEach((navRoot) => {
    const toggle = navRoot.querySelector("[data-nav-toggle]");
    const panel = navRoot.querySelector("[data-nav-panel]");

    if (!toggle || !panel) {
      return;
    }

    const compactQuery = window.matchMedia("(max-width: 1180px)");

    const closePanel = ({ restoreFocus = false } = {}) => {
      toggle.setAttribute("aria-expanded", "false");
      panel.hidden = true;
      if (restoreFocus) {
        toggle.focus();
      }
    };

    const syncNav = () => {
      if (compactQuery.matches) {
        if (toggle.getAttribute("aria-expanded") !== "true") {
          panel.hidden = true;
        }
      } else {
        closePanel();
      }
    };

    toggle.addEventListener("click", () => {
      const nextExpanded = toggle.getAttribute("aria-expanded") !== "true";
      toggle.setAttribute("aria-expanded", String(nextExpanded));
      panel.hidden = !nextExpanded;
    });

    panel.querySelectorAll("a").forEach((link) => {
      link.addEventListener("click", () => {
        if (compactQuery.matches) {
          closePanel();
        }
      });
    });

    document.addEventListener("keydown", (event) => {
      if (event.key === "Escape" && compactQuery.matches && !panel.hidden) {
        closePanel({ restoreFocus: true });
      }
    });

    document.addEventListener("click", (event) => {
      if (!compactQuery.matches || panel.hidden) {
        return;
      }
      const target = event.target;
      if (target instanceof Node && !navRoot.contains(target)) {
        closePanel();
      }
    });

    if (typeof compactQuery.addEventListener === "function") {
      compactQuery.addEventListener("change", syncNav);
    } else if (typeof compactQuery.addListener === "function") {
      compactQuery.addListener(syncNav);
    }

    window.addEventListener("resize", syncNav);

    if (typeof ResizeObserver === "function") {
      const navResizeObserver = new ResizeObserver(() => {
        syncNav();
      });
      navResizeObserver.observe(document.documentElement);
    }

    syncNav();
  });
})();
