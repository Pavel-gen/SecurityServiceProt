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

    // --- ОБРАБОТКА ЛОКАЛЬНЫХ СУЩНОСТЕЙ ---
    if (entity.source === 'local' && entity.sourceTable) {
        // Приоритетный локальный ключ (в зависимости от таблицы)
        let localId = null;
        let idType = '';

        // Определяем ID и его тип в зависимости от таблицы
        switch (entity.sourceTable) {
            case 'CF_Contacts_test':
                localId = entity.PersonUNID;
                idType = 'PersonUNID';
                break;
            case 'CF_Persons_test':
                localId = entity.UNID;
                idType = 'UNID';
                break;
            case 'CF_PrevWork_test':
                // Для PrevWork может быть логичнее использовать комбинацию PersonUNID + INN или другое
                // Но если нужен уникальный ключ, можно использовать PersonUNID
                localId = entity.PersonUNID;
                idType = 'PersonUNID';
                break;
            case 'CI_ContPersons_test':
                localId = entity.cpUID;
                idType = 'cpUID';
                break;
            case 'CI_Contragent_test':
                localId = entity.UNID;
                idType = 'UNID';
                break;
            case 'CI_Employees_test':
                localId = entity.fzUID;
                idType = 'fzUID';
                break;
            default:
                // Резервный вариант: попробовать стандартные поля
                localId = entity.UNID || entity.fzUID || entity.cpUID || entity.PersonUNID;
                // Если используем резервный, всё равно нужно указать тип для уникальности
                idType = 'generic';
        }

        if (localId) {
            // Генерируем составной ключ: sourceTable + '_' + idType + '_' + localId
            // Это гарантирует уникальность даже при совпадении ID между таблицами
            return `${entity.sourceTable}_${idType}_${localId}`;
        }
    }

    // --- ОБРАБОТКА СУЩНОСТЕЙ ИЗ DELTA ---
    if (entity.source === 'delta') {
        // Для юрлиц/ИП из эндпоинта company
        if (entity.sourceEndpoint === 'company' && entity.deltaRaw && entity.deltaRaw.company_id) {
            return `delta_company_${entity.deltaRaw.company_id}`;
        }
        // Для ФЛ из эндпоинта person
        if (entity.sourceEndpoint === 'person' && entity.deltaRaw && entity.deltaRaw.person_id) {
            return `delta_person_${entity.deltaRaw.person_id}`;
        }
        // Для ИП из эндпоинта ip (предполагаем, что там тоже может быть id)
        if (entity.sourceEndpoint === 'ip' && entity.deltaRaw && entity.deltaRaw.id) {
            return `delta_ip_${entity.deltaRaw.id}`;
        }
        // Резервный вариант: использовать ИНН как ключ (менее надёжно, если ИНН не уникальны в контексте Delta)
        if (entity.INN) {
            return `delta_inn_${entity.INN}`;
        }
    }

    // Если не найден ни один ключ, возвращаем null
    return null;
}


export {determineEntityType, cleanPhone, getEntityKey, normalizePhoneSQL};