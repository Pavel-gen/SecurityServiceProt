// main.js
import { performSearch } from './api.js';
import { setState, setResults, setCurrentTab, getCurrentTab } from './state.js';
import { updateTabBadges, updateResultsCount, toggleEmptyState, switchTab, toggleAdvancedSearch, changeSort, toggleSortDirection, applyFilters, exportData } from './ui.js';
import { displayResults } from './render.js';

async function handleSearch() {
    const searchQuery = document.getElementById('searchInput').value.trim();
    const data = await performSearch(searchQuery);

    if (data) {
        setResults({
            juridical: data.juridical || [],
            physical: data.physical || [],
            ip: data.ip || []
        });

        updateTabBadges();
        updateResultsCount();

        const totalResults = data.juridical.length + data.physical.length + data.ip.length;
        toggleEmptyState(totalResults === 0);

        displayResults(getCurrentTab());
    } else {
        document.querySelectorAll('.results-section').forEach(section => {
            section.style.display = 'none';
        });
        toggleEmptyState(true);
    }
}

document.addEventListener('DOMContentLoaded', function() {
    // Назначаем обработчики событий
    document.getElementById('searchBtn').addEventListener('click', handleSearch);
    document.getElementById('advancedSearchBtn').addEventListener('click', toggleAdvancedSearch);
    document.getElementById('exportBtn').addEventListener('click', exportData);
    document.getElementById('sortSelect').addEventListener('change', changeSort);
    document.getElementById('sortDirectionBtn').addEventListener('click', toggleSortDirection);

    document.getElementById('typeFilter').addEventListener('change', applyFilters);
    document.getElementById('connectionFilter').addEventListener('change', applyFilters);
    document.getElementById('statusFilter').addEventListener('change', applyFilters);

    document.querySelectorAll('.tab').forEach(tab => {
        tab.addEventListener('click', () => {
            const tabName = tab.getAttribute('data-tab');
            setCurrentTab(tabName);
            switchTab(tabName);
        });
    });

    document.getElementById('searchInput').addEventListener('keypress', (e) => {
        if (e.key === 'Enter') {
            handleSearch();
        }
    });

    // Инициализируем UI
    updateTabBadges();
    updateResultsCount();
    toggleEmptyState(true);

    // Сделаем toggleSection глобально доступной для HTML
    window.toggleSection = import('./ui.js').then(({ toggleSection }) => {
        window.toggleSection = toggleSection;
    });
});