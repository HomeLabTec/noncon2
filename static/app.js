function applyTheme(isDark) {
    document.body.classList.toggle('dark-mode', isDark);
}

function initDarkMode() {
    const toggle = document.getElementById('dark-mode-toggle');
    if (!toggle) return;
    const stored = localStorage.getItem('noncon-theme');
    const prefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;
    const isDark = stored ? stored === 'dark' : prefersDark;
    applyTheme(isDark);
    toggle.addEventListener('click', () => {
        const currentlyDark = document.body.classList.toggle('dark-mode');
        localStorage.setItem('noncon-theme', currentlyDark ? 'dark' : 'light');
    });
}

function initOtherSelects() {
    document.querySelectorAll('select[data-has-other]')
        .forEach(select => {
            const fieldName = select.getAttribute('name');
            const otherInput = document.getElementById(`${fieldName}_other`);
            const updateVisibility = () => {
                if (!otherInput) return;
                const show = select.value === '__other__';
                otherInput.hidden = !show;
                if (show) {
                    otherInput.focus({ preventScroll: true });
                } else {
                    otherInput.value = '';
                }
            };
            select.addEventListener('change', updateVisibility);
            // Initialize on load
            if (select.value === '__other__') {
                otherInput && (otherInput.hidden = false);
            }
        });
}

function initPrintView() {
    const printSheets = Array.from(document.querySelectorAll('.print-sheet'));
    const printButtons = Array.from(document.querySelectorAll('[data-print-target]'));
    if (!printButtons.length || !printSheets.length) return;

    const cleanup = () => {
        document.body.classList.remove('show-print-preview');
        printSheets.forEach(sheet => sheet.setAttribute('aria-hidden', 'true'));
    };

    printButtons.forEach(btn => {
        const targetId = btn.getAttribute('data-print-target');
        const target = document.getElementById(targetId);
        if (!target) return;

        btn.addEventListener('click', (event) => {
            event.preventDefault();
            document.body.classList.add('show-print-preview');
            printSheets.forEach(sheet => sheet.setAttribute('aria-hidden', sheet === target ? 'false' : 'true'));
            target.scrollIntoView({ behavior: 'smooth', block: 'start' });
            // Allow the preview to render before invoking the print dialog.
            setTimeout(() => window.print(), 50);
        });
    });

    window.addEventListener('afterprint', cleanup);
}

document.addEventListener('DOMContentLoaded', () => {
    initDarkMode();
    initOtherSelects();
    initPrintView();
});
