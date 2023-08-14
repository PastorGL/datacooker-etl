function selectLocale() {
    const location = window.location;

    for (let lang in window.navigator.languages) {
        if (lang.startsWith('en')) {
            location.href = location.href.replace('index.html', 'gh-pages/en/index.html');
        }
        if (lang.startsWith('ru')) {
            location.href = location.href.replace('index.html', 'gh-pages/ru/index.html');
        }
    }

    location.href = location.href.replace('index.html', 'gh-pages/en/index.html');
}