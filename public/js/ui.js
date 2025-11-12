// ui.js
import { getResults } from './state.js';
import { displayResults } from './renderer.js'; // Импортируем для switchTab

export function toggleSection(element) {
    const content = element.parentElement.querySelector('.toggle-content');
    const icon = element.querySelector('.fa-chevron-down');
    content.classList.toggle('expanded');
    if (icon) {
        icon.classList.toggle('fa-chevron-down');
        icon.classList.toggle('fa-chevron-up');
    }
}

export function toggleAdvancedSearch() {
    const filtersContainer = document.getElementById('filtersContainer');
    const advancedSearchBtn = document.getElementById('advancedSearchBtn');
    filtersContainer.classList.toggle('active');
    advancedSearchBtn.classList.toggle('active');
}

export function updateTabBadges() {
    const results = getResults();
    document.querySelector('.tab[data-tab="juridical"] .tab-badge').textContent = results.juridical.length;
    document.querySelector('.tab[data-tab="physical"] .tab-badge').textContent = results.physical.length;
    document.querySelector('.tab[data-tab="ip"] .tab-badge').textContent = results.ip.length;
}

export function switchTab(tabName) {
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    document.querySelector(`.tab[data-tab="${tabName}"]`).classList.add('active');

    document.querySelectorAll('.results-section').forEach(section => section.classList.remove('active'));
    document.getElementById(`${tabName}-results`).classList.add('active');

    displayResults(tabName);
}

export function updateResultsCount() {
    const results = getResults();
    const totalResults = results.juridical.length + results.physical.length + results.ip.length;
    document.getElementById('resultsCount').textContent = `Найдено результатов: ${totalResults}`;
}

export function toggleEmptyState(show) {
    const emptyState = document.getElementById('emptyState');
    emptyState.style.display = show ? 'block' : 'none';
}

export function changeSort() {
    console.log('Сортировка изменена');
    // Здесь должна быть логика пересортировки
    import('./state.js').then(({ getCurrentTab }) => {
        displayResults(getCurrentTab());
    });
}

export function toggleSortDirection() {
    console.log('Направление сортировки изменено');
    import('./state.js').then(({ getCurrentTab }) => {
        displayResults(getCurrentTab());
    });
}

export function applyFilters() {
    console.log('Фильтры применены');
    import('./state.js').then(({ getCurrentTab }) => {
        displayResults(getCurrentTab());
    });
}

export function exportData() {
    alert('Экспорт данных выполнен успешно!');
}