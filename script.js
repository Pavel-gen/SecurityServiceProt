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

        // Сохраняем полученные результаты
        appState.results.juridical = data.juridical || [];
        appState.results.physical = data.physical || [];
        appState.results.ip = data.ip || [];

        // Обновляем счетчики вкладок
        updateTabBadges();

        // Показываем результаты в активной вкладке
        displayResults(appState.currentTab);

        // Обновляем счетчик найденных результатов
        const totalResults = appState.results.juridical.length + appState.results.physical.length + appState.results.ip.length;
        document.getElementById('resultsCount').textContent = `Найдено результатов: ${totalResults}`;

        // Скрываем пустое состояние, если есть результаты
        emptyState.style.display = totalResults > 0 ? 'none' : 'block';

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
    // Это базовая версия, которая создает карточки с основной информацией
    // Проверяет наличие и непустоту полей перед отображением
    // И добавляет блок с JSON-данными для отладки
    let title, icon, status = '', additionalInfo = '', connections = '';

    if (tabName === 'juridical') {
        // Приоритет: NameFull (полное название), затем NameShort (короткое название), затем INN
        title = item.NameFull || item.NameShort || item.INN || 'Нет названия';
        icon = 'fas fa-building';
        // Определяем статус: если fIP = 1, то это ИП, иначе юрлицо
        // Используем type, установленный сервером
        status = item.type === 'ip' ? `<span class="card-status status-active">Действующее (ИП)</span>` : // Если тип ip, то статус ИП
            `<span class="card-status status-active">Действующее</span>`; // Пример статуса для юрлица

        // Собираем информацию, проверяя наличие и непустоту полей
        let innInfo = '';
        if (item.INN && item.INN.trim() !== '') {
            innInfo = `
                <div class="info-item">
                    <div class="info-icon"><i class="fas fa-id-card"></i></div>
                    <div class="info-content">
                        <div class="info-label">ИНН</div>
                        <div class="info-value">${item.INN}</div>
                    </div>
                </div>
            `;
        }

        let ogrnInfo = '';
        if (item.OGRN && item.OGRN.trim() !== '') {
            ogrnInfo = `
                <div class="info-item">
                    <div class="info-icon"><i class="fas fa-id-card"></i></div>
                    <div class="info-content">
                        <div class="info-label">ОГРН</div>
                        <div class="info-value">${item.OGRN}</div>
                    </div>
                </div>
            `;
        }

        let addressUrInfo = '';
        if (item.AddressUr && item.AddressUr.trim() !== '') {
            addressUrInfo = `
                <div class="info-item">
                    <div class="info-icon"><i class="fas fa-map-marker-alt"></i></div>
                    <div class="info-content">
                        <div class="info-label">Юр. адрес</div>
                        <div class="info-value">${item.AddressUr}</div>
                    </div>
                </div>
            `;
        }

        let addressFaktInfo = '';
        if (item.AddressFakt && item.AddressFakt.trim() !== '') {
            addressFaktInfo = `
                <div class="info-item">
                    <div class="info-icon"><i class="fas fa-map-marker-alt"></i></div>
                    <div class="info-content">
                        <div class="info-label">Факт. адрес</div>
                        <div class="info-value">${item.AddressFakt}</div>
                    </div>
                </div>
            `;
        }

        let phoneInfo = '';
        if (item.PhoneNum && item.PhoneNum.trim() !== '') {
            phoneInfo = `
                <div class="info-item">
                    <div class="info-icon"><i class="fas fa-phone"></i></div>
                    <div class="info-content">
                        <div class="info-label">Телефон</div>
                        <div class="info-value">${item.PhoneNum}</div>
                    </div>
                </div>
            `;
        }

        let emailInfo = '';
        if (item.eMail && item.eMail.trim() !== '') {
            emailInfo = `
                <div class="info-item">
                    <div class="info-icon"><i class="fas fa-envelope"></i></div>
                    <div class="info-content">
                        <div class="info-label">Email</div>
                        <div class="info-value">${item.eMail}</div>
                    </div>
                </div>
            `;
        }

        // Добавляем другие возможные поля из CI_Contragent, если они нужны и не пусты
        // Пример для KPP:
        let kppInfo = '';
        if (item.KPP && item.KPP.trim() !== '') {
            kppInfo = `
                <div class="info-item">
                    <div class="info-icon"><i class="fas fa-id-card"></i></div>
                    <div class="info-content">
                        <div class="info-label">КПП</div>
                        <div class="info-value">${item.KPP}</div>
                    </div>
                </div>
            `;
        }

        additionalInfo = innInfo + ogrnInfo + kppInfo + addressUrInfo + addressFaktInfo + phoneInfo + emailInfo; // Объединяем все блоки

        // connections пока пустой, можно добавить логику, если данные о связях придут отдельно
    } else if (tabName === 'physical') {
        // Проверяем, откуда пришли данные (CI_Employees, CI_Contragent как физлицо)
        // Используем доступные поля, приоритет fzFIO (из CI_Employees), затем NameShort (из CI_Contragent), затем INN
        title = item.fzFIO || item.NameShort || item.INN || 'Нет ФИО';

        // Определяем иконку: если это сотрудник (есть phOrgName), иначе физлицо
        icon = item.phOrgName ? 'fas fa-user-tie' : 'fas fa-user';

        // Собираем информацию, проверяя наличие и непустоту полей
        let innInfo = '';
        if (item.INN && item.INN.trim() !== '') {
            innInfo = `
                <div class="info-item">
                    <div class="info-icon"><i class="fas fa-id-card"></i></div>
                    <div class="info-content">
                        <div class="info-label">ИНН</div>
                        <div class="info-value">${item.INN}</div>
                    </div>
                </div>
            `;
        }

        let phoneInfo = '';
        // Ищем телефон в разных возможных полях (из CI_Employees и CI_Contragent)
        const phoneValue = item.fzPhoneM || item.PhoneNum; // Добавьте другие возможные поля, например, cpPhoneMob, cpPhoneWork из CI_ContPersons если вернете её
        if (phoneValue && phoneValue.trim() !== '') {
            phoneInfo = `
                <div class="info-item">
                    <div class="info-icon"><i class="fas fa-phone"></i></div>
                    <div class="info-content">
                        <div class="info-label">Телефон</div>
                        <div class="info-value">${phoneValue}</div>
                    </div>
                </div>
            `;
        }

        let emailInfo = '';
        // Ищем email в разных возможных полях (из CI_Employees и CI_Contragent)
        const emailValue = item.fzMail || item.eMail; // Добавьте cpMail если вернете CI_ContPersons
        if (emailValue && emailValue.trim() !== '') {
            emailInfo = `
                <div class="info-item">
                    <div class="info-icon"><i class="fas fa-envelope"></i></div>
                    <div class="info-content">
                        <div class="info-label">Email</div>
                        <div class="info-value">${emailValue}</div>
                    </div>
                </div>
            `;
        }

        let addressInfo = '';
        // Ищем адреса в разных возможных полях (из CI_Employees и CI_Contragent)
        const addrUrValue = item.fzAddress || item.AddressUr; // Добавьте cpAddress если вернете CI_ContPersons
        const addrFaktValue = item.fzAddressF || item.AddressUFakt; // Добавьте cpAddress если вернете CI_ContPersons
        if (addrUrValue && addrUrValue.trim() !== '') {
            addressInfo += `
                <div class="info-item">
                    <div class="info-icon"><i class="fas fa-map-marker-alt"></i></div>
                    <div class="info-content">
                        <div class="info-label">Адрес (рег.)</div>
                        <div class="info-value">${addrUrValue}</div>
                    </div>
                </div>
            `;
        }
        if (addrFaktValue && addrFaktValue.trim() !== '') {
            addressInfo += `
                <div class="info-item">
                    <div class="info-icon"><i class="fas fa-map-marker-alt"></i></div>
                    <div class="info-content">
                        <div class="info-label">Адрес (факт.)</div>
                        <div class="info-value">${addrFaktValue}</div>
                    </div>
                </div>
            `;
        }

        additionalInfo = innInfo + phoneInfo + emailInfo + addressInfo;

        // Добавляем поля, специфичные для CI_Employees, если они есть и не пусты
        if (item.phOrgName && item.phOrgName.trim() !== '') {
             additionalInfo += `
                <div class="info-item">
                    <div class="info-icon"><i class="fas fa-building"></i></div>
                    <div class="info-content">
                        <div class="info-label">Организация</div>
                        <div class="info-value">${item.phOrgName}</div>
                    </div>
                </div>
            `;
        }
        if (item.phOrgINN && item.phOrgINN.trim() !== '') {
             additionalInfo += `
                <div class="info-item">
                    <div class="info-icon"><i class="fas fa-id-card"></i></div>
                    <div class="info-content">
                        <div class="info-label">ИНН организации</div>
                        <div class="info-value">${item.phOrgINN}</div>
                    </div>
                </div>
            `;
        }
        // Добавьте другие поля из CI_Employees, если нужно
        if (item.fzDateB) { // Предположим, дата рождения может быть нужна
            additionalInfo += `
                <div class="info-item">
                    <div class="info-icon"><i class="fas fa-calendar"></i></div>
                    <div class="info-content">
                        <div class="info-label">Дата рождения</div>
                        <div class="info-value">${item.fzDateB}</div>
                    </div>
                </div>
            `;
        }

    } else if (tabName === 'ip') { // Для данных из CI_Contragent с fIP=1 (возвращаются с type='ip')
        // Приоритет: NameFull (полное название), затем NameShort (короткое название), затем INN
        title = item.NameFull || item.NameShort || item.INN || 'Нет названия ИП';
        icon = 'fas fa-store';
        status = `<span class="card-status status-active">Действующий (ИП)</span>`; // Пример статуса

        // Собираем информацию, проверяя наличие и непустоту полей (аналогично юрлицу)
        let innInfo = '';
        if (item.INN && item.INN.trim() !== '') {
            innInfo = `
                <div class="info-item">
                    <div class="info-icon"><i class="fas fa-id-card"></i></div>
                    <div class="info-content">
                        <div class="info-label">ИНН</div>
                        <div class="info-value">${item.INN}</div>
                    </div>
                </div>
            `;
        }

        let ogrnInfo = '';
        if (item.OGRN && item.OGRN.trim() !== '') {
            ogrnInfo = `
                <div class="info-item">
                    <div class="info-icon"><i class="fas fa-id-card"></i></div>
                    <div class="info-content">
                        <div class="info-label">ОГРНИП</div>
                        <div class="info-value">${item.OGRN}</div>
                    </div>
                </div>
            `;
        }

        let addressInfo = '';
        if (item.AddressUr && item.AddressUr.trim() !== '') {
            addressInfo = `
                <div class="info-item">
                    <div class="info-icon"><i class="fas fa-map-marker-alt"></i></div>
                    <div class="info-content">
                        <div class="info-label">Адрес</div>
                        <div class="info-value">${item.AddressUr}</div>
                    </div>
                </div>
            `;
        }

        let phoneInfo = '';
        if (item.PhoneNum && item.PhoneNum.trim() !== '') {
            phoneInfo = `
                <div class="info-item">
                    <div class="info-icon"><i class="fas fa-phone"></i></div>
                    <div class="info-content">
                        <div class="info-label">Телефон</div>
                        <div class="info-value">${item.PhoneNum}</div>
                    </div>
                </div>
            `;
        }

        let emailInfo = '';
        if (item.eMail && item.eMail.trim() !== '') {
            emailInfo = `
                <div class="info-item">
                    <div class="info-icon"><i class="fas fa-envelope"></i></div>
                    <div class="info-content">
                        <div class="info-label">Email</div>
                        <div class="info-value">${item.eMail}</div>
                    </div>
                </div>
            `;
        }

        additionalInfo = innInfo + ogrnInfo + addressInfo + phoneInfo + emailInfo;
    }

    // --- ГЕНЕРАЦИЯ БЛОКА СВЯЗЕЙ ---
        let connectionsBlock = '<p>Связей не найдено.</p>'; // Значение по умолчанию
    let connectionCount = 0; // Счетчик связей для бейджа

    if (item.connections && Array.isArray(item.connections) && item.connections.length > 0) {
        const allConnectionsHtml = [];

        item.connections.forEach(connectionGroup => {
            // Обработка связей по email
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
            // Обработка связей по телефону
            else if (connectionGroup.type === 'contact' && connectionGroup.subtype === 'phone' && Array.isArray(connectionGroup.connections)) {
                const contactValue = connectionGroup.contact; // Например, '+79991112233'
                const connectionsList = connectionGroup.connections;

                let contactSectionHtml = `
                    <div class="info-item">
                        <div class="info-icon"><i class="fas fa-phone"></i></div> <!-- Используем иконку телефона -->
                        <div class="info-content">
                            <div class="info-label">Телефон</div> <!-- Метка "Телефон" -->
                            <div class="info-value">${contactValue}</div> <!-- Значение телефона -->
                        </div>
                    </div>
                    <div class="connections-details-container"> <!-- Контейнер для связанных сущностей -->
                `;

                connectionsList.forEach(conn => {
                    const connectedEntity = conn.connectedEntity;
                    const connectedName = connectedEntity.NameShort || connectedEntity.NameFull || connectedEntity.INN || 'N/A';
                    const connectedType = connectedEntity.type || 'unknown';

                    let connectedIcon = 'fas fa-question-circle';
                    if (connectedType === 'juridical') connectedIcon = 'fas fa-building';
                    else if (connectedType === 'ip') connectedIcon = 'fas fa-store';
                    else if (connectedType === 'physical') connectedIcon = 'fas fa-user';

                    // Добавляем элемент связи с отступом и символом
                    contactSectionHtml += `
                        <div class="connection-detail"> <!-- Класс для отступа и стиля -->
                            <i class="${connectedIcon}"></i> <!-- Иконка связанной сущности -->
                            ${connectedName} (${conn.connectionDetails}) <!-- Имя и детали -->
                        </div>
                    `;
                    connectionCount++;
                });
                contactSectionHtml += '</div>'; // Закрываем контейнер
                allConnectionsHtml.push(contactSectionHtml);
            }
            // else if ... (другие типы связей)
        });

        if (allConnectionsHtml.length > 0) {
            connectionsBlock = allConnectionsHtml.join('');
        }
    }

    // --- КОНЕЦ ГЕНЕРАЦИИ БЛОКА СВЯЗЕЙ ---

    // --- Добавляем блок с JSON данными ---
    // Экранируем кавычки и переводы строк для использования в HTML атрибуте и вставке в <pre>
    const itemJsonString = JSON.stringify(item, null, 2) // Форматируем JSON с отступами
        .replace(/&/g, '&amp;') // Экранируем &
        .replace(/</g, '<')  // Экранируем <
        .replace(/>/g, '>'); // Экранируем >

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

    // --- ВСТАВКА connectionsBlock и connectionCount в HTML карточки ---
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
                        ${jsonInfoBlock} <!-- Добавляем блок JSON здесь -->
                        <p>Дополнительная информация будет загружаться сюда.</p>
                    </div>
                </div>
                <div class="toggle-section">
                    <div class="toggle-header" onclick="toggleSection(this)">
                        <div class="toggle-title">
                            <i class="fas fa-chevron-down"></i>
                            <span>Связи</span>
                            <span class="toggle-badge">${connectionCount}</span> <!-- Вставляем счетчик -->
                        </div>
                    </div>
                    <div class="toggle-content">
                        ${connectionsBlock} <!-- Вставляем сгенерированный блок связей -->
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