function determineEntityType(entity) {
    if (!entity) return 'unknown';
    
    // Если тип уже определен - используем его
    if (entity.type) return entity.type;
    
    // Определяем по UrFiz и fIP
    if (entity.fIP === 1 || entity.fIP === true) return 'ip';
    if (entity.UrFiz === 1) return 'juridical';
    if (entity.UrFiz === 2) return 'physical';
    
    // Определяем по длине ИНН
    if (entity.INN) {
        if (entity.INN.length === 10) return 'juridical';
        if (entity.INN.length === 12) return 'physical';
    }
    
    // Определяем по sourceTable как fallback
    switch(entity.sourceTable) {
        case 'contragent':
        case 'contperson':
            return 'juridical';
        case 'employee':
        case 'person_direct_inn_match':
        case 'prevwork_by_org_inn':
            return 'physical';
        default:
            return 'unknown';
    }
}

function cleanPhone(phone) {
    // Убираем все нецифровые символы, кроме +
    return phone.replace(/[^\d]/g, '');
}

function normalizePhoneSQL(columnName) {
    return `REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(${columnName}, '+', ''), '(', ''), ')', ''), '-', ''), ' ', '')`;
}

function getEntityKey(entity) {
    if (!entity) {
        return null;
    }

    // --- ОБРАБОТКА DELTA СУЩНОСТЕЙ ---
    if (entity.source === 'delta') {
        if (entity.INN) {
            return `delta_${entity.type}_${entity.INN}`;
        }
        if (entity.NameShort) {
            return `delta_${entity.type}_${entity.NameShort.replace(/\s+/g, '_')}`;
        }
    }

    // --- ОБРАБОТКА ЛОКАЛЬНЫХ СУЩНОСТЕЙ ---
    if (entity.source === 'local' && entity.sourceTable) {
        let localId = null;
        let idType = '';

        // НОРМАЛИЗУЕМ НАЗВАНИЯ ТАБЛИЦ
        const normalizedTable = normalizeTableName(entity.sourceTable);
        
        switch (normalizedTable) {
            case 'ci_contragent_test':
                localId = entity.UNID || entity.contactUNID;
                idType = 'UNID';
                break;
            case 'ci_contpersons_test':
                localId = entity.cpUID || entity.contactUNID;
                idType = 'cpUID';
                break;
            case 'ci_employees_test':
                localId = entity.fzUID || entity.contactUNID;
                idType = 'fzUID';
                break;
            case 'cf_persons_test':
                localId = entity.UNID || entity.PersonUNID;
                idType = 'UNID';
                break;
            case 'cf_contacts_test':
                localId = entity.PersonUNID || entity.contactUNID;
                idType = 'PersonUNID';
                break;
            case 'cf_prevwork_test':
                localId = entity.PersonUNID;
                idType = 'PersonUNID';
                break;
            default:
                localId = entity.UNID || entity.fzUID || entity.cpUID || entity.PersonUNID || entity.contactUNID;
                idType = 'generic';
        }

        if (localId) {
            const key = `${normalizedTable}_${idType}_${localId}`;
            return key;
        } else {
            console.log('getEntityKey: не найден localId для', normalizedTable);
        }
    }

    // Fallback
    return entity.PersonUNID || entity.UNID || entity.fzUID || entity.cpUID || entity.INN;
}

// Новая функция для нормализации названий таблиц
function normalizeTableName(tableName) {
    if (!tableName) return tableName;
    
    const tableMapping = {
        'contragent': 'ci_contragent_test',
        'CI_Contragent_test': 'ci_contragent_test',
        'contperson': 'ci_contpersons_test', 
        'CI_ContPersons_test': 'ci_contpersons_test',
        'employee': 'ci_employees_test',
        'CI_Employees_test': 'ci_employees_test',
        'CF_Persons_test': 'cf_persons_test',
        'CF_Contacts_test': 'cf_contacts_test',
        'CF_PrevWork_test': 'cf_prevwork_test',
        'person_direct_inn_match': 'cf_persons_test',
        'person_by_inn_via_prevwork': 'cf_persons_test',
        'prevwork_by_org_inn': 'cf_prevwork_test',
        'employee_by_person_inn': 'ci_employees_test'
    };
    
    const normalized = tableMapping[tableName] || tableName.toLowerCase();
    // console.log(`🔧 Нормализация таблицы: ${tableName} -> ${normalized}`);
    return normalized;
}


export {determineEntityType, cleanPhone, getEntityKey, normalizePhoneSQL};