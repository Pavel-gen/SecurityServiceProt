// state.js
// Состояние приложения
let appState = {
    currentTab: 'juridical',
    results: {
        juridical: [],
        physical: [],
        ip: []
    }
};

export function getState() {
    return appState;
}

export function setState(newState) {
    appState = { ...appState, ...newState };
}

export function setCurrentTab(tabName) {
    appState.currentTab = tabName;
}

export function getCurrentTab() {
    return appState.currentTab;
}

export function setResults(results) {
    appState.results = results;
}

export function getResults() {
    return appState.results;
}

export function getResultsForTab(tabName) {
    return appState.results[tabName] || [];
}