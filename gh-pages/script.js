function selectLocale() {
    let located = window.location.href.toString();
    if (!located.endsWith("index.html")) {
        located += "index.html";
    }

    located = location.href.replace('index.html', 'gh-pages/en/index.html');
    for (let lang of window.navigator.languages) {
        if (lang.startsWith('en')) {
            located = location.href.replace('index.html', 'gh-pages/en/index.html');
        }
        if (lang.startsWith('ru')) {
            located = location.href.replace('index.html', 'gh-pages/ru/index.html');
        }
    }

    location.href = located;
}