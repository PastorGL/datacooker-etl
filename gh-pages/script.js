function selectLocale() {
    let located = window.location.href.toString();
    if (!located.endsWith("index.html")) {
        located += "index.html";
    }

    let localeFound = false;
    for (let lang of window.navigator.languages) {
        if (lang.startsWith('en')) {
            located = located.replace('index.html', 'gh-pages/en/index.html');
            localeFound = true;
            break;
        }
        if (lang.startsWith('ru')) {
            located = located.replace('index.html', 'gh-pages/ru/index.html');
            localeFound = true;
            break;
        }
    }

    if (!localeFound) {
        located = located.replace('index.html', 'gh-pages/en/index.html');
    }

    window.location.href = located;
}