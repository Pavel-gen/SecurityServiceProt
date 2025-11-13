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
        // console.log('getEntityKey: entity is null/undefined');
        return null;
    }

    // --- ОБРАБОТКА ЛОКАЛЬНЫХ СУЩНОСТЕЙ ---
    if (entity.source === 'local' && entity.sourceTable) {
        let localId = null;
        let idType = '';

        // console.log('getEntityKey для:', {
        //     sourceTable: entity.sourceTable,
        //     UNID: entity.UNID,
        //     fzUID: entity.fzUID, 
        //     cpUID: entity.cpUID,
        //     PersonUNID: entity.PersonUNID,
        //     contactUNID: entity.contactUNID
        // });

        switch (entity.sourceTable) {
            case 'CI_Contragent_test':
                localId = entity.UNID || entity.contactUNID;
                idType = 'UNID';
                break;
            case 'CI_ContPersons_test':
                localId = entity.cpUID || entity.contactUNID;
                idType = 'cpUID';
                break;
            case 'CI_Employees_test':
                localId = entity.fzUID || entity.contactUNID;
                idType = 'fzUID';
                break;
            case 'CF_Persons_test':
                localId = entity.UNID || entity.PersonUNID;
                idType = 'UNID';
                break;
            case 'CF_Contacts_test':
                localId = entity.PersonUNID || entity.contactUNID;
                idType = 'PersonUNID';
                break;
            default:
                localId = entity.UNID || entity.fzUID || entity.cpUID || entity.PersonUNID || entity.contactUNID;
                idType = 'generic';
        }

        if (localId) {
            const key = `${entity.sourceTable}_${idType}_${localId}`;
            // console.log('Сгенерирован ключ:', key);
            return key;
        } else {
            console.log('getEntityKey: не найден localId для', entity.sourceTable);
        }
    }

    // console.log('getEntityKey: fallback для', entity);
    return entity.PersonUNID || entity.UNID || entity.fzUID || entity.cpUID || entity.INN;
}


export {determineEntityType, cleanPhone, getEntityKey, normalizePhoneSQL};