function determineEntityType(UrFiz, fIP) {
    if (fIP === 1) return 'ip';
    if (UrFiz === 1) return 'juridical';
    if (UrFiz === 2) return 'physical';
    return 'unknown';
}

function cleanPhone(phone) {
    // Убираем все нецифровые символы, кроме +
    return phone.replace(/[^\d]/g, '');
}

function normalizePhoneSQL(columnName) {
    return `REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(${columnName}, '+', ''), '(', ''), ')', ''), '-', ''), ' ', '')`;
}

function getEntityKey(entity) {
    // Сначала проверяем локальные ключи
    const localKey = entity.UNID || entity.fzUID || entity.cpUID || entity.PersonUNID;
    if (localKey) {
        return localKey;
    }

    // Если локальных ключей нет, проверяем на источник Delta
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
        if (entity.sourceEndpoint === 'ip' && entity.deltaRaw && entity.deltaRaw.id) { // Уточните поле id для IP
            return `delta_ip_${entity.deltaRaw.id}`;
        }
        // Резервный вариант: использовать ИНН как ключ (менее надёжно, если ИНН не уникальны в контексте Delta)
        if (entity.INN) {
            return `delta_inn_${entity.INN}`;
        }
    }

    // Если не найден ни один ключ, возвращаем null или генерируем ошибку
    return null;
}

export {determineEntityType, cleanPhone, getEntityKey, normalizePhoneSQL};