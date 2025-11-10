// URL вашего сервера
const API_URL = 'http://localhost:3000/api/search';

// Состояние приложения
let appState = {
    currentTab: 'juridical',
    results: {
        juridical: [],
        physical: [],
        ip: []
    }
};

// Функция переключения секций (раскрывающиеся блоки)
function toggleSection(element) {
    const content = element.parentElement.querySelector('.toggle-content');
    const icon = element.querySelector('.fa-chevron-down');
    content.classList.toggle('expanded');
    if (icon) {
        icon.classList.toggle('fa-chevron-down');
        icon.classList.toggle('fa-chevron-up');
    }
}

// Функция для показа/скрытия расширенного поиска
function toggleAdvancedSearch() {
    const filtersContainer = document.getElementById('filtersContainer');
    const advancedSearchBtn = document.getElementById('advancedSearchBtn');
    filtersContainer.classList.toggle('active');
    advancedSearchBtn.classList.toggle('active');
}

// Функция для выполнения поиска
async function performSearch() {
    const searchQuery = document.getElementById('searchInput').value.trim();
    const emptyState = document.getElementById('emptyState');

    if (searchQuery.length < 3) {
        alert('Введите не менее 3 символов для поиска.');
        return;
    }

    try {
        // Показываем индикатор загрузки (опционально)
        // console.log('Запрос к серверу...');

        const response = await fetch(API_URL, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ query: searchQuery })
        });

        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }

        const data = await response.json();
        console.log('Данные с сервера:', data); // Для отладки

        // --- НАЧАЛО: Обработка и объединение результатов ---
        // 1. Сохраняем результаты из локальной БД
        appState.results.juridical = data.juridical || [];
        appState.results.physical = data.physical || [];
        appState.results.ip = data.ip || [];

        // 2. Обрабатываем и добавляем результаты из Delta к juridical
        if (Array.isArray(data.delta_results)) {
            const deltaAsJuridical = data.delta_results.map(item => {
                // Маппинг полей из Delta в структуру, понятную createCardHtml и совместимую с локальной БД
                return {
                    // Стандартные поля для createCardHtml
                    INN: item.inn,
                    OGRN: item.ogrn,
                    KPP: item.kpp,
                    NameFull: item.full_name,
                    NameShort: item.short_name,
                    AddressUr: item.register_address,
                    PhoneNum: item.PhoneNum || item.fnsAdress_kpp?.phone || item.fnsAdress_inn?.phone || '', // Приоритет: маппинг из Delta, затем телефон ИФНС
                    eMail: item.email || '', // Если есть в Delta
                    // Дополнительные поля из Delta
                    status_from_delta: item.status,
                    charter_capital: item.charter_capital,
                    main_activity: item.main_activity,
                    register_date: item.register_date,
                    register_type: item.register_type,
                    register_address: item.register_address,
                    // ... другие поля из Delta, если нужно ...
                    // Поле, указывающее источник
                    source: 'delta',
                    // Оригинальные данные Delta для отладки (опционально)
                    // delta_info: item
                };
            });
            // Добавляем маппированные результаты Delta к juridical
            appState.results.juridical = appState.results.juridical.concat(deltaAsJuridical);
        }
        // --- КОНЕЦ: Обработка и объединение результатов ---

        // Обновляем счетчики вкладок (только для juridical, physical, ip)
        updateTabBadges();

        // Показываем результаты в активной вкладке
        displayResults(appState.currentTab);

        // Обновляем счетчик найденных результатов (только для juridical, physical, ip, теперь включая Delta)
        const totalResults = appState.results.juridical.length + appState.results.physical.length + appState.results.ip.length;
        document.getElementById('resultsCount').textContent = `Найдено результатов: ${totalResults}`;

        // Скрываем пустое состояние, если есть результаты (учитывая объединённые juridical)
        const hasAnyResults = totalResults > 0;
        emptyState.style.display = hasAnyResults ? 'none' : 'block';

    } catch (error) {
        console.error('Ошибка при поиске:', error);
        alert('Произошла ошибка при выполнении поиска. Проверьте консоль.');
        // Скрываем результаты при ошибке
        document.querySelectorAll('.results-section').forEach(section => {
            section.style.display = 'none';
        });
        emptyState.style.display = 'block';
    }
}

// Функция обновления счетчиков на вкладках
function updateTabBadges() {
    document.querySelector('.tab[data-tab="juridical"] .tab-badge').textContent = appState.results.juridical.length;
    document.querySelector('.tab[data-tab="physical"] .tab-badge').textContent = appState.results.physical.length;
    document.querySelector('.tab[data-tab="ip"] .tab-badge').textContent = appState.results.ip.length;
}

// Функция для отображения результатов в указанной вкладке
function displayResults(tabName) {
    const containerId = `${tabName}Container`;
    const container = document.getElementById(containerId);

    // Очищаем контейнер
    container.innerHTML = '';

    // Получаем данные для текущей вкладки
    const results = appState.results[tabName];

    if (!results || results.length === 0) {
        container.innerHTML = '<p>Нет данных для отображения.</p>';
        return;
    }

    // Формируем HTML для карточек на основе полученных данных
    results.forEach(item => {
        const cardHtml = createCardHtml(item, tabName);
        container.insertAdjacentHTML('beforeend', cardHtml);
    });
}


// Функция создания HTML-кода для карточки (адаптированная для отображения только непустых полей)
// ... (ваш существующий код script.js до функции createCardHtml) ...

// Функция создания HTML-кода для карточки (адаптированная для отображения только непустых полей)
function createCardHtml(item, tabName) {
    // Определяем тип и соответствующие иконки/статусы
    let icon = 'fas fa-question-circle';
    let status = '';
    let title = '';

    // Приоритет: NameFull, затем NameShort, затем INN
    title = item.NameFull || item.NameShort || item.INN || item.full_name || item.short_name || 'Нет названия';

    // Определяем иконку по типу (если тип задан сервером) или по другим признакам
    if (item.type === 'ip' || item.fIP === 1) {
        icon = 'fas fa-store';
    } else if (item.type === 'physical' || item.UrFiz === 2) {
        // Проверяем, является ли это сотрудником
        icon = item.phOrgName ? 'fas fa-user-tie' : 'fas fa-user';
    } else if (item.type === 'juridical' || item.UrFiz === 1) {
        icon = 'fas fa-building';
    } else {
        // Если тип неясен, пробуем определить по наличию ИНН/ФИО
        if (item.INN) {
            // Предполагаем, что если есть ИНН и это не из CI_Employees (нет fzFIO), то юрлицо
            if (item.fzFIO === undefined) {
                icon = 'fas fa-building';
            } else {
                icon = 'fas fa-user'; // Физлицо из CI_Employees
            }
        }
    }

    // Определяем статус
    // Приоритет: статус из Delta (status_from_delta или status из delta_info)
    const deltaStatus = item.status_from_delta || (item.delta_info && item.delta_info.status);
    if (deltaStatus) {
        // Примеры статусов из Delta: "Действующее", "Ликвидируется", "Исключение из ЕГРЮЛ..."
        let statusClass = 'status-active';
        if (deltaStatus.toLowerCase().includes('ликвид') || deltaStatus.toLowerCase().includes('исключ')) {
            statusClass = 'status-liquidated'; // Можно добавить CSS-класс для ликвидированных
        }
        status = `<span class="card-status ${statusClass}">${deltaStatus}</span>`;
    } else if (item.type === 'ip') {
        status = `<span class="card-status status-active">Действующий (ИП)</span>`;
    } else if (item.type === 'juridical') {
        status = `<span class="card-status status-active">Действующее</span>`;
    }

    // --- ОПРЕДЕЛЯЕМ ПОЛЯ ДЛЯ ОТОБРАЖЕНИЯ ---
    // Массив пар [отображаемое_имя, имя_поля_в_объекте, иконка]
    // Порядок важен. Поля, которых нет в объекте, будут пропущены.
    const fieldsToDisplay = [
        // Основные поля (локальные или объединённые)
        ['ИНН', 'INN', 'fas fa-id-card'],
        ['ОГРН', 'OGRN', 'fas fa-id-card'], // или 'ОГРНИП' для ИП
        ['КПП', 'KPP', 'fas fa-id-card'], // только для юрлиц
        // Поля из Delta (если объединены)
        ['ИНН', 'inn', 'fas fa-id-card'], // Альтернативное имя из Delta
        ['ОГРН', 'ogrn', 'fas fa-id-card'],
        ['КПП', 'kpp', 'fas fa-id-card'],
        ['Полное наименование', 'full_name', 'fas fa-building'], // из Delta
        ['Краткое наименование', 'short_name', 'fas fa-building'], // из Delta
        ['Фирменное наименование', 'firm_name', 'fas fa-building'], // из Delta
        ['Статус', 'status_from_delta', 'fas fa-info-circle'], // из Delta
        ['Статус (Delta)', 'status', 'fas fa-info-circle'], // если брали из delta_info.status
        ['Дата регистрации', 'register_date', 'fas fa-calendar'],
        ['Тип регистрации', 'register_type', 'fas fa-file-alt'],
        ['Уставный капитал', 'charter_capital', 'fas fa-money-bill-wave'],
        ['Основной вид деятельности', 'main_activity', 'fas fa-industry'],
        ['Адрес регистрации', 'register_address', 'fas fa-map-marker-alt'],
        ['Юр. адрес', 'AddressUr', 'fas fa-map-marker-alt'], // локальное поле
        ['Факт. адрес', 'AddressUFakt', 'fas fa-map-marker-alt'], // локальное поле
        ['Телефон', 'PhoneNum', 'fas fa-phone'], // локальное поле
        ['Телефон (Delta)', 'phone', 'fas fa-phone'], // если будет из Delta
        ['Email', 'eMail', 'fas fa-envelope'], // локальное поле
        ['Email (Delta)', 'email', 'fas fa-envelope'], // если будет из Delta
        ['ОКПО', 'okpo', 'fas fa-barcode'], // из Delta
        ['ИНН организации (сотр.)', 'phOrgINN', 'fas fa-building'], // для сотрудников
        ['Организация (сотр.)', 'phOrgName', 'fas fa-building'], // для сотрудников
        ['ФИО (сотр.)', 'fzFIO', 'fas fa-user'], // для сотрудников
        ['Дата рождения (сотр.)', 'fzDateB', 'fas fa-calendar'], // для сотрудников
        // Добавьте другие поля по необходимости
    ];

    // --- ГЕНЕРИРУЕМ HTML ДЛЯ ОСНОВНОЙ ИНФОРМАЦИИ ---
    let additionalInfo = '';
    fieldsToDisplay.forEach(([label, field, iconClass]) => {
        // Проверяем наличие значения в объединённом объекте
        // Приоритет: объединённое поле (например, item.NameFull), затем поле из delta_info (item.delta_info?.NameFull)
        const value = item[field] || (item.delta_info && item.delta_info[field]);
        if (value && value.toString().trim() !== '') { // Проверяем, что значение есть и не пустая строка
            additionalInfo += `
                <div class="info-item">
                    <div class="info-icon"><i class="${iconClass}"></i></div>
                    <div class="info-content">
                        <div class="info-label">${label}</div>
                        <div class="info-value">${value}</div>
                    </div>
                </div>
            `;
        }
    });

    // --- ГЕНЕРИРУЕМ БЛОК СВЯЗЕЙ (как и раньше) ---
     // --- ГЕНЕРИРУЕМ БЛОК СВЯЗЕЙ (как и раньше) ---
    let connectionsBlock = '<p>Связей не найдено.</p>';
    let connectionCount = 0;

    if (item.connections && Array.isArray(item.connections) && item.connections.length > 0) {
        const allConnectionsHtml = [];

        item.connections.forEach(connectionGroup => {
            if (connectionGroup.type === 'contact' && connectionGroup.subtype === 'email' && Array.isArray(connectionGroup.connections)) {
                const contactValue = connectionGroup.contact;
                const connectionsList = connectionGroup.connections;

                let contactSectionHtml = `
                    <div class="info-item">
                        <div class="info-icon"><i class="fas fa-envelope"></i></div>
                        <div class="info-content">
                            <div class="info-label">Email</div>
                            <div class="info-value">${contactValue}</div>
                        </div>
                    </div>
                    <div class="connections-details-container">
                `;

                connectionsList.forEach(conn => {
                    const connectedEntity = conn.connectedEntity;
                    const connectedName = connectedEntity.NameShort || connectedEntity.NameFull || connectedEntity.INN || 'N/A';
                    const connectedType = connectedEntity.type || 'unknown';

                    let connectedIcon = 'fas fa-question-circle';
                    if (connectedType === 'juridical') connectedIcon = 'fas fa-building';
                    else if (connectedType === 'ip') connectedIcon = 'fas fa-store';
                    else if (connectedType === 'physical') connectedIcon = 'fas fa-user';

                    contactSectionHtml += `
                        <div class="connection-detail">
                            <i class="${connectedIcon}"></i>
                            ${connectedName} (${conn.connectionDetails})
                        </div>
                    `;
                    connectionCount++;
                });
                contactSectionHtml += '</div>';
                allConnectionsHtml.push(contactSectionHtml);
            }
            else if (connectionGroup.type === 'contact' && connectionGroup.subtype === 'phone' && Array.isArray(connectionGroup.connections)) {
                const contactValue = connectionGroup.contact;
                const connectionsList = connectionGroup.connections;

                let contactSectionHtml = `
                    <div class="info-item">
                        <div class="info-icon"><i class="fas fa-phone"></i></div>
                        <div class="info-content">
                            <div class="info-label">Телефон</div>
                            <div class="info-value">${contactValue}</div>
                        </div>
                    </div>
                    <div class="connections-details-container">
                `;

                connectionsList.forEach(conn => {
                    const connectedEntity = conn.connectedEntity;
                    const connectedName = connectedEntity.NameShort || connectedEntity.NameFull || connectedEntity.INN || 'N/A';
                    const connectedType = connectedEntity.type || 'unknown';

                    let connectedIcon = 'fas fa-question-circle';
                    if (connectedType === 'juridical') connectedIcon = 'fas fa-building';
                    else if (connectedType === 'ip') connectedIcon = 'fas fa-store';
                    else if (connectedType === 'physical') connectedIcon = 'fas fa-user';

                    contactSectionHtml += `
                        <div class="connection-detail">
                            <i class="${connectedIcon}"></i>
                            ${connectedName} (${conn.connectionDetails})
                        </div>
                    `;
                    connectionCount++;
                });
                contactSectionHtml += '</div>';
                allConnectionsHtml.push(contactSectionHtml);
            }
            // --- НОВЫЙ БЛОК: Обработка связей по ИНН ---
            else if (connectionGroup.type === 'inn' && connectionGroup.subtype === 'inn_match' && Array.isArray(connectionGroup.connections)) {
                const innValue = connectionGroup.contact; // ИНН, по которому была найдена связь
                const connectionsList = connectionGroup.connections;

                // НЕ группируем связи, а отображаем каждую отдельно
                // let innSectionHtml = `<div class="info-item"><div class="info-icon"><i class="fas fa-id-card"></i></div><div class="info-content"><div class="info-label">ИНН: ${innValue}</div>`; // Включаем ИНН в лейбл
                let innSectionHtml = `
                    <div class="info-item">
                        <div class="info-icon"><i class="fas fa-id-card"></i></div>
                        <div class="info-content">
                            <div class="info-label">ИНН</div>
                            <div class="info-value">${item.INN}</div>
                        </div>
                    </div>
                    <div class="connections-details-container">
                `;

                // Перебираем каждую связь отдельно
                connectionsList.forEach(conn => {
                    const connectedEntity = conn.connectedEntity;
                    const connectedName = connectedEntity.NameShort || connectedEntity.NameFull || connectedEntity.INN || 'N/A';
                    const connectedType = connectedEntity.type || 'unknown';
                    const connectionStatus = conn.connectionStatus;
                    const sourceTable = connectedEntity.sourceTable;
                    const baseName = connectedEntity.baseName; // Имя БД источника

                    let connectedIcon = 'fas fa-question-circle';
                    if (connectedType === 'juridical') connectedIcon = 'fas fa-building';
                    else if (connectedType === 'ip') connectedIcon = 'fas fa-store';
                    else if (connectedType === 'physical') connectedIcon = 'fas fa-user';

                    // Определяем человекочитаемое описание статуса
                    let statusDescription = 'Статус неизвестен';
                    switch (connectionStatus) {
                        case 'organization_match':
                            statusDescription = 'Совпадение по ИНН организации';
                            break;
                        case 'former_employee':
                            statusDescription = 'Бывший сотрудник';
                            break;
                        case 'current_employee':
                            statusDescription = 'Текущий сотрудник';
                            break;
                        case 'contact_person':
                            statusDescription = 'Контактное лицо';
                            break;
                        default:
                            statusDescription = connectionStatus; // Используем как есть, если не распознали
                    }

                    // Определяем человекочитаемое описание источника
                    let sourceDescription = 'Источник неизвестен';
                    switch (sourceTable) {
                        case 'contragent':
                            sourceDescription = 'Данные из реестра контрагентов';
                            break;
                        case 'prevwork':
                            sourceDescription = 'Данные из предыдущих мест работы';
                            break;
                        case 'employee':
                            sourceDescription = 'Данные из сотрудников';
                            break;
                        case 'contperson':
                            sourceDescription = 'Данные из контактных лиц';
                            break;
                        default:
                            sourceDescription = sourceTable; // Используем как есть, если не распознали
                    }

                    // Формируем описание источника БД
                    const baseNameDescription = baseName ? `BD: ${baseName}` : 'BD: не указана';

                    innSectionHtml += `
                        <div class="connection-detail">
                            <i class="${connectedIcon}"></i>
                            ${connectedName} (${statusDescription}, ${sourceDescription}, ${baseNameDescription})
                        </div>
                    `;
                    connectionCount++; // Увеличиваем счетчик на 1 за каждую связь
                });

                innSectionHtml += '</div>';
                allConnectionsHtml.push(innSectionHtml);
            }
            // else if ... (другие типы связей)
        });

        if (allConnectionsHtml.length > 0) {
            connectionsBlock = allConnectionsHtml.join('');
        }
    }
    // --- КОНЕЦ ГЕНЕРАЦИИ БЛОКА СВЯЗЕЙ ---

    // --- ДОБАВЛЯЕМ БЛОК JSON (как и раньше) ---
    const itemJsonString = JSON.stringify(item, null, 2)
        .replace(/&/g, '&amp;')
        .replace(/</g, '<')
        .replace(/>/g, '>');

    const jsonInfoBlock = `
        <div class="info-item">
            <div class="info-icon"><i class="fas fa-database"></i></div>
            <div class="info-content">
                <div class="info-label">Сырые данные (JSON)</div>
                <div class="info-value">
                    <pre style="max-height: 150px; overflow-y: auto; background-color: #f5f5f5; padding: 10px; border-radius: 4px; white-space: pre-wrap; word-wrap: break-word; font-size: 12px; line-height: 1.4;">${itemJsonString}</pre>
                </div>
            </div>
        </div>
    `;

    // --- ВОЗВРАЩАЕМ HTML КАРТОЧКИ ---
    return `
        <div class="card">
            <div class="card-header">
                <div class="card-title">
                    <i class="${icon}"></i>
                    ${title}
                    ${status}
                </div>
                <div class="card-actions">
                    <button class="card-action-btn"><i class="far fa-star"></i></button>
                    <button class="card-action-btn"><i class="fas fa-external-link-alt"></i></button>
                    <button class="card-action-btn"><i class="fas fa-download"></i></button>
                </div>
            </div>
            <div class="card-content">
                <div class="basic-info">
                    ${additionalInfo}
                </div>
                <div class="toggle-section">
                    <div class="toggle-header" onclick="toggleSection(this)">
                        <div class="toggle-title">
                            <i class="fas fa-chevron-down"></i>
                            <span>Подробная информация</span>
                        </div>
                    </div>
                    <div class="toggle-content">
                        ${jsonInfoBlock}
                        <p>Дополнительная информация будет загружаться сюда.</p>
                    </div>
                </div>
                <div class="toggle-section">
                    <div class="toggle-header" onclick="toggleSection(this)">
                        <div class="toggle-title">
                            <i class="fas fa-chevron-down"></i>
                            <span>Связи</span>
                            <span class="toggle-badge">${connectionCount}</span>
                        </div>
                    </div>
                    <div class="toggle-content">
                        ${connectionsBlock}
                    </div>
                </div>
            </div>
        </div>
    `;
}


// ... (остальной код script.js остается без изменений) ...


// Функция для переключения вкладок
function switchTab(tabName) {
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

    // Обновляем текущую вкладку в состоянии
    appState.currentTab = tabName;

    // Отображаем результаты для новой вкладки
    displayResults(tabName);

    // Если это вкладка с графом, перерисовываем граф (заглушка)
    if (tabName === 'graph') {
        // drawConnectionGraph(); // Пока закомментировано, так как реализация сложная
        document.getElementById('connectionGraph').innerHTML = '<p>Граф связей будет отрисован здесь.</p>';
    }
}

// Функция изменения сортировки (заглушка)
function changeSort() {
    console.log('Сортировка изменена');
    // Здесь должна быть логика пересортировки appState.results[appState.currentTab]
    displayResults(appState.currentTab); // Обновляем отображение
}

// Функция изменения направления сортировки (заглушка)
function toggleSortDirection() {
    console.log('Направление сортировки изменено');
    // Здесь должна быть логика изменения направления
    displayResults(appState.currentTab); // Обновляем отображение
}

// Функция применения фильтров (заглушка)
function applyFilters() {
    console.log('Фильтры применены');
    // Здесь должна быть логика фильтрации appState.results
    displayResults(appState.currentTab); // Обновляем отображение
}

// Функция экспорта данных (заглушка)
function exportData() {
    alert('Экспорт данных выполнен успешно!');
}

// Инициализация при загрузке
document.addEventListener('DOMContentLoaded', function() {
    // Назначаем обработчики событий
    document.getElementById('searchBtn').addEventListener('click', performSearch);
    document.getElementById('advancedSearchBtn').addEventListener('click', toggleAdvancedSearch);
    document.getElementById('exportBtn').addEventListener('click', exportData);
    document.getElementById('sortSelect').addEventListener('change', changeSort);
    document.getElementById('sortDirectionBtn').addEventListener('click', toggleSortDirection);

    // Обработчики для фильтров
    document.getElementById('typeFilter').addEventListener('change', applyFilters);
    document.getElementById('connectionFilter').addEventListener('change', applyFilters);
    document.getElementById('statusFilter').addEventListener('change', applyFilters);

    // Обработчики для вкладок
    document.querySelectorAll('.tab').forEach(tab => {
        tab.addEventListener('click', () => {
            const tabName = tab.getAttribute('data-tab');
            switchTab(tabName);
        });
    });

    // Обработчик Enter в поле поиска
    document.getElementById('searchInput').addEventListener('keypress', (e) => {
        if (e.key === 'Enter') {
            performSearch();
        }
    });

    // Инициализируем счетчики вкладок (0 при загрузке)
    updateTabBadges();
});