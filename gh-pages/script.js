function selectLocale() {
    const location = window.location;

    for (let lang in window.navigator.languages) {
        if (lang.startsWith('en')) {
            location.href = location.href.replace('index.html', 'gh-pages/index-EN.html');
        }
        if (lang.startsWith('ru')) {
            location.href = location.href.replace('index.html', 'gh-pages/index-RU.html');
        }
    }

    location.href = location.href.replace('index.html', 'gh-pages/index-EN.html');
}