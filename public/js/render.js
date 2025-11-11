// renderer.js
import { getResultsForTab } from './state.js';
import { toggleSection } from './ui.js'; // Импортируем для использования в createCardHtml

export function displayResults(tabName) {
    const containerId = `${tabName}Container`;
    const container = document.getElementById(containerId);

    container.innerHTML = '';

    const results = getResultsForTab(tabName);

    if (!results || results.length === 0) {
        container.innerHTML = '<p>Нет данных для отображения.</p>';
        return;
    }

    results.forEach(item => {
        const cardHtml = createCardHtml(item, tabName);
        container.insertAdjacentHTML('beforeend', cardHtml);
    });
}

function createCardHtml(item, tabName) {
    // --- ВАША СЛОЖНАЯ ЛОГИКА createCardHtml ОСТАЕТСЯ ЗДЕСЬ ---
    // ... (скопируйте сюда вашу функцию createCardHtml из script.js, включая генерацию связей и JSON) ...
    // Основные изменения:
    // 1. Убедитесь, что toggleSection доступна: window.toggleSection = toggleSection; или используйте addEventListener внутри этой функции.
    // 2. Убедитесь, что все переменные и функции, используемые внутри, определены или импортированы.

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

    // --- ОПРЕДЕЛЯЕМ ИСТОЧНИК ---
    let sourceInfo = '';
    if (item.source === 'local') {
        const tableName = item.sourceTable || 'Неизвестная таблица';
        const dbName = item.baseName || 'Неизвестная БД';
        sourceInfo = `<div class="info-item"><div class="info-icon"><i class="fas fa-database"></i></div><div class="info-content"><div class="info-label">Источник</div><div class="info-value">Локально, Таблица: ${tableName}, БД: ${dbName}</div></div></div>`;
    } else if (item.source === 'delta') {
        const endpoint = item.sourceEndpoint || 'Неизвестный эндпоинт';
        sourceInfo = `<div class="info-item"><div class="info-icon"><i class="fas fa-external-link-alt"></i></div><div class="info-content"><div class="info-label">Источник</div><div class="info-value">Delta Безопасность (${endpoint})</div></div></div>`;
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

    // --- ГЕНЕРИРУЕМ БЛОК СВЯЗЕЙ (обновлённый) ---
    let connectionsBlock = '<p>Связей не найдено.</p>';
    let connectionCount = 0;

    if (item.connections && Array.isArray(item.connections) && item.connections.length > 0) {
        const allConnectionsHtml = [];

        item.connections.forEach(connectionGroup => {
            // ... (логика для email и phone, если нужна) ...

            // --- НОВЫЙ БЛОК: Обработка связей по ИНН ---
            if (connectionGroup.type === 'inn' && connectionGroup.subtype === 'inn_match' && Array.isArray(connectionGroup.connections)) {
                const innValue = connectionGroup.contact; // ИНН, по которому была найдена связь
                const connectionsList = connectionGroup.connections;

                let innSectionHtml = `
                    <div class="info-item">
                        <div class="info-icon"><i class="fas fa-id-card"></i></div>
                        <div class="info-content">
                            <div class="info-label">Связи по ИНН</div>
                            <div class="info-value">${innValue}</div>
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
                    const source = connectedEntity.source; // Источник: local или delta

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

                    // Определяем человекочитаемое описание источника и БД
                    let sourceDescription = 'Источник неизвестен';
                    let baseNameDescription = '';
                    if (source === 'local') {
                        switch (sourceTable) {
                            case 'contragent':
                                sourceDescription = 'Контрагент';
                                break;
                            case 'prevwork':
                                sourceDescription = 'Предыдущее место работы';
                                break;
                            case 'employee':
                                sourceDescription = 'Сотрудник';
                                break;
                            case 'contperson':
                                sourceDescription = 'Таблица контактных лиц';
                                break;
                            default:
                                sourceDescription = sourceTable; // Используем как есть, если не распознали
                        }
                        baseNameDescription = baseName ? ` (БД: ${baseName})` : ' (БД: не указана)';
                    } else if (source === 'delta') {
                        sourceDescription = 'Delta Безопасность';
                        baseNameDescription = ''; // Delta не имеет BaseName
                    }

                    const connectionDetails = conn.connectionDetails; // Основная строка
                    const employeeInfo = conn.employeeInfo; // Структурированные данные сотрудника

                    // ... (определение иконки, статуса, источника) ...

                    // --- НАЧАЛО: Формирование HTML для деталей сотрудника ---
                    let employeeDetailsHtml = '';
                    if (employeeInfo) { // Проверяем, есть ли объект employeeInfo
                        employeeDetailsHtml = '<div class="employee-details">'; // Контейнер для деталей сотрудника
                        if (employeeInfo.phFunction) {
                            employeeDetailsHtml += `<div class="emp-position">Должность: ${employeeInfo.phFunction}</div>`;
                        }
                        if (employeeInfo.phEventType) {
                            employeeDetailsHtml += `<div class="emp-event">Событие: ${employeeInfo.phEventType}</div>`;
                        }
                        if (employeeInfo.phDate) {
                            employeeDetailsHtml += `<div class="emp-date">Дата: ${employeeInfo.phDate}</div>`;
                        }
                        // Добавьте другие поля, если нужно
                        employeeDetailsHtml += '</div>';
                    }
                    // --- КОНЕЦ: Формирование HTML для деталей сотрудника ---

                    // Формируем основной HTML для связи
                    innSectionHtml += `
                        <div class="connection-detail">
                            <i class="${connectedIcon}"></i>
                            <div class="connection-info">
                                <div class="connected-name">${connectedName}</div>
                                <div class="connection-meta">(${statusDescription}, ${sourceDescription}${baseNameDescription})</div>
                                <!-- Вставляем детали сотрудника, если они есть -->
                                ${employeeDetailsHtml}
                            </div>
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
                    ${sourceInfo} <!-- Добавляем информацию об источнике -->
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