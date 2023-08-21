function selectLocale() {
    const location = window.location;

    let located = location.href.replace('index.html', 'gh-pages/en/index.html');
    for (let lang in window.navigator.languages) {
        if (lang.startsWith('en')) {
            located = location.href.replace('index.html', 'gh-pages/en/index.html');
        }
        if (lang.startsWith('ru')) {
            located = location.href.replace('index.html', 'gh-pages/ru/index.html');
        }
    }

    location.href = located;
}