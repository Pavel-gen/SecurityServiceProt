// ui.js
import { getResults, getCurrentTab } from './state.js';
import { displayResults } from './render.js'; // Импортируем для вызова в switchTab

// Функция переключения секций (раскрывающиеся блоки)
export function toggleSection(element) {
    const content = element.parentElement.querySelector('.toggle-content');
    const icon = element.querySelector('.fa-chevron-down');
    content.classList.toggle('expanded');
    if (icon) {
        icon.classList.toggle('fa-chevron-down');
        icon.classList.toggle('fa-chevron-up');
    }
}

// Функция для показа/скрытия расширенного поиска
export function toggleAdvancedSearch() {
    const filtersContainer = document.getElementById('filtersContainer');
    const advancedSearchBtn = document.getElementById('advancedSearchBtn');
    filtersContainer.classList.toggle('active');
    advancedSearchBtn.classList.toggle('active');
}

// Функция обновления счетчиков на вкладках
export function updateTabBadges() {
    const results = getResults();
    document.querySelector('.tab[data-tab="juridical"] .tab-badge').textContent = results.juridical.length;
    document.querySelector('.tab[data-tab="physical"] .tab-badge').textContent = results.physical.length;
    document.querySelector('.tab[data-tab="ip"] .tab-badge').textContent = results.ip.length;
}

// Функция для переключения вкладок
export function switchTab(tabName) {
    // Убираем активный класс у всех вкладок
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    // Добавляем активный класс текущей вкладке
    document.querySelector(`.tab[data-tab="${tabName}"]`).classList.add('active');

    // Скрываем все секции с результатами
    document.querySelectorAll('.results-section').forEach(section => {
        section.classList.remove('active');
    });

    // Показываем нужную секцию
    document.getElementById(`${tabName}-results`).classList.add('active');

    // Отображаем результаты для новой вкладки
    displayResults(tabName);
}

// Функция обновления счетчика найденных результатов
export function updateResultsCount() {
    const results = getResults();
    const totalResults = results.juridical.length + results.physical.length + results.ip.length;
    document.getElementById('resultsCount').textContent = `Найдено результатов: ${totalResults}`;
}

// Функция показа/скрытия пустого состояния
export function toggleEmptyState(show) {
    const emptyState = document.getElementById('emptyState');
    emptyState.style.display = show ? 'block' : 'none';
}

// Функция изменения сортировки (заглушка)
export function changeSort() {
    console.log('Сортировка изменена');
    // Здесь должна быть логика пересортировки appState.results[appState.currentTab]
    displayResults(getCurrentTab()); // Обновляем отображение
}

// Функция изменения направления сортировки (заглушка)
export function toggleSortDirection() {
    console.log('Направление сортировки изменено');
    // Здесь должна быть логика изменения направления
    displayResults(getCurrentTab()); // Обновляем отображение
}

// Функция применения фильтров (заглушка)
export function applyFilters() {
    console.log('Фильтры применены');
    // Здесь должна быть логика фильтрации appState.results
    displayResults(getCurrentTab()); // Обновляем отображение
}

// Функция экспорта данных (заглушка)
export function exportData() {
    alert('Экспорт данных выполнен успешно!');
}