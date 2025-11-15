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

function createCardHtml(item) {
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

let connectionsBlock = '<p>Связей не найдено.</p>';
let connectionCount = 0;

if (item.connections && Array.isArray(item.connections) && item.connections.length > 0) {
    
    // 1. ГРУППИРУЕМ: ТИП -> ЗНАЧЕНИЕ -> СВЯЗИ
    const groupedData = {};
    
    item.connections.forEach(connectionGroup => {
        const type = connectionGroup.type;
        const value = connectionGroup.contact;
        
        if (!groupedData[type]) {
            groupedData[type] = {};
        }
        if (!groupedData[type][value]) {
            groupedData[type][value] = [];
        }
        
        if (Array.isArray(connectionGroup.connections)) {
            groupedData[type][value].push(...connectionGroup.connections);
        }
    });

    // 2. КОНФИГУРАЦИЯ ТИПОВ
    const typeConfigs = {
        'contact': {
            icon: 'fas fa-envelope',
            label: 'Email'
        },
        'inn': {
            icon: 'fas fa-id-card',
            label: 'ИНН'
        },
        'phone': {
            icon: 'fas fa-phone',
            label: 'Телефоны'
        }
    };

    // 3. СОЗДАЕМ HTML С ВЛОЖЕННОЙ СТРУКТУРОЙ
    const allGroupsHtml = [];

    Object.entries(groupedData).forEach(([type, valuesMap]) => {
        const config = typeConfigs[type];
        if (!config) return;

        const values = Object.keys(valuesMap);
        const valuesCount = values.length;

        let groupHtml = `
            <div class="connection-type">
                <i class="${config.icon}"></i>
                ${config.label} (${valuesCount})
            </div>
            <div class="connections-details-container">
        `;

        // ДЛЯ КАЖДОГО ЗНАЧЕНИЯ (email/inn/phone)
        Object.entries(valuesMap).forEach(([value, connections]) => {
            // Заголовок значения (уровень 1)
            groupHtml += `
                <div class="connection-detail level-1">
                    <span class="connection-value-main">${value} →</span>
                </div>
            `;

            // Все связи для этого значения (уровень 2)
            connections.forEach(conn => {
                const connectedEntity = conn.connectedEntity;
                const connectedName = connectedEntity.NameShort || connectedEntity.NameFull || connectedEntity.INN || 'N/A';
                const connectionDetails = conn.connectionDetails || 'Нет описания связи';
                const source = connectedEntity.source;
                const baseName = connectedEntity.baseName;

                // Определяем иконку сущности
                function getEntityIcon(entity) {
                    // Если тип явно указан
                    if (entity.type === 'organization' || entity.type === 'juridical') {
                        return 'fas fa-building';
                    }
                    if (entity.type === 'physical') {
                        return 'fas fa-user';
                    }
                    if (entity.type === 'ip') {
                        return 'fas fa-store';
                    }
                    if (entity.type === 'contact') {
                        return 'fas fa-address-card';
                    }
                    
                    // Определяем по UrFiz
                    if (entity.UrFiz === 1) {
                        return 'fas fa-building'; // Юрлицо
                    }
                    if (entity.UrFiz === 2) {
                        return 'fas fa-user'; // Физлицо
                    }
                    
                    // Определяем по fIP (ИП)
                    if (entity.fIP === true || entity.fIP === 1) {
                        return 'fas fa-store';
                    }
                    
                    // По умолчанию
                    return 'fas fa-question-circle';
                }

                // Используем так:
let entityIcon = getEntityIcon(connectedEntity);

                // Форматируем connectionDetails
                let cleanDetails = connectionDetails;
                if (connectionDetails.includes(value)) {
                    cleanDetails = connectionDetails.replace(new RegExp(`${value}[,\\s]*`, 'gi'), '').trim();
                    cleanDetails = cleanDetails.replace(/^,\s*/, '');
                }

                // Формат источника
                let sourceText = '';
                if (source === 'local') {
                    sourceText = baseName ? ` (БД: ${baseName})` : '';
                } else if (source === 'delta') {
                    sourceText = ' (Delta Безопасность)';
                }

                groupHtml += `
                    <div class="connection-detail level-2">
                        <i class="${entityIcon}"></i>
                        <span class="connection-value">${connectedName}</span>
                        <span class="connection-comment">(${cleanDetails}${sourceText})</span>
                    </div>
                `;
                
                connectionCount++;
            });

            // Добавляем отступ между группами значений
            groupHtml += `<div class="connection-spacer"></div>`;
        });

        groupHtml += `
            </div>
        `;
        
        allGroupsHtml.push(groupHtml);
    });

    if (allGroupsHtml.length > 0) {
        connectionsBlock = allGroupsHtml.join('');
    }
}    // --- ДОБАВЛЯЕМ БЛОК JSON (как и раньше) ---
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

// Конфигурация для типов сущностей
const ENTITY_TYPES = {
    juridical: { icon: 'fas fa-building', label: 'Юридическое лицо' },
    ip: { icon: 'fas fa-store', label: 'ИП' },
    physical: { icon: 'fas fa-user', label: 'Физическое лицо' },
    contact: { icon: 'fas fa-address-card', label: 'Контакт' },
    unknown: { icon: 'fas fa-question-circle', label: 'Неизвестно' }
};

// Конфигурация для sourceTable
const SOURCE_TABLE_CONFIG = {
    // Локальные источники
    contragent: { label: 'Контрагент', category: 'local' },
    employee: { label: 'Сотрудник', category: 'local' },
    contperson: { label: 'Таблица контактных лиц', category: 'local' },
    person_direct_inn_match: { label: 'Персоны (по ИНН)', category: 'local' },
    prevwork_by_org_inn: { label: 'Персоны (через пред. работу)', category: 'local' },
    employee_by_person_inn: { label: 'Сотрудники (по ИНН физлица)', category: 'local' },
    person_by_inn_via_prevwork: { label: 'Персоны (через пред. работу, по ИНН)', category: 'local' },
    prevwork_person_from_persons: { label: 'Предыдущее место работы', category: 'local' },
    
    // Контакты
    contact: { label: 'Таблица контактов', category: 'local' },
    person_via_contact: { label: 'Персоны (через контакты)', category: 'local' },
    employee_via_contact: { label: 'Сотрудники (через контакты)', category: 'local' },
    contperson_via_contact: { label: 'Конт.лица (через контакты)', category: 'local' },
    person_from_prevwork_email: { label: 'Персоны (email в пред. работе)', category: 'local' },
    person_from_prevwork_via_contact: { label: 'Персоны (через пред. работу)', category: 'local' }
};

// Конфигурация для статусов связей
const CONNECTION_STATUS_CONFIG = {
    organization_match: 'Совпадение по ИНН организации',
    former_employee: 'Бывший сотрудник',
    current_employee: 'Текущий сотрудник',
    contact_person: 'Контактное лицо',
    former_employee_of: 'Работал в',
    current_employee_of: 'Работает в',
    former_workplace_of: 'Бывшее место работы',
    person_match: 'Найден как физическое лицо'
};

// Конфигурация для типов контактов
const CONTACT_TYPES = {
    email: { label: 'Связи по Email', icon: 'fas fa-envelope' },
    phone: { label: 'Связи по Телефону', icon: 'fas fa-phone' },
    inn: { label: 'Связи по ИНН', icon: 'fas fa-id-card' }
};