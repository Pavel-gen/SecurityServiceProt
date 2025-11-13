// helpers.js

function determineEntityTypeSpec(connectedEntity) {
    let entityType = 'unknown';
    
    // Если UrFiz и fIP есть (не NULL), используем их для определения типа
    if (connectedEntity.UrFiz !== null && connectedEntity.UrFiz !== undefined) {
        if (connectedEntity.UrFiz === 1) {
            // Юрлицо
            if (connectedEntity.fIP === 1 || connectedEntity.fIP === true) {
                entityType = 'ip';
            } else {
                entityType = 'juridical';
            }
        } else if (connectedEntity.UrFiz === 2) {
            // Физлицо
            entityType = 'physical';
        }
    } else {
        // Если UrFiz нет (например, для таблицы contact), определяем по sourceTable
        switch(connectedEntity.sourceTable) {
            case 'contact':
                entityType = 'contact';
                break;
            case 'person_via_contact':
            case 'person_from_prevwork_via_contact':
            case 'employee':
            case 'employee_via_contact':
            case 'person_from_prevwork_phone':
                entityType = 'physical';
                break;
            case 'contragent':
            case 'contperson':
            case 'contperson_via_contact':
                if (connectedEntity.fIP === 1 || connectedEntity.fIP === true) {
                    entityType = 'ip';
                } else if (connectedEntity.INN && connectedEntity.INN.length === 10) {
                    entityType = 'juridical';
                } else if (connectedEntity.INN && connectedEntity.INN.length === 12) {
                    entityType = 'physical';
                }
                break;
            default:
                // Fallback
                if (connectedEntity.INN && connectedEntity.INN.length === 10) {
                    entityType = 'juridical';
                } else if (connectedEntity.INN && connectedEntity.INN.length === 12) {
                    if (connectedEntity.fIP === 1 || connectedEntity.fIP === true) {
                        entityType = 'ip';
                    } else {
                        entityType = 'physical';
                    }
                } else if (connectedEntity.NameShort || connectedEntity.NameFull) {
                    entityType = 'physical';
                }
        }
    }
    
    return entityType;
}

function cleanPhone(phone) {
    // Убираем все нецифровые символы, кроме +
    return phone.replace(/[^\d]/g, '');
}

function normalizePhoneSQL(columnName) {
    return `REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(${columnName}, '+', ''), '(', ''), ')', ''), '-', ''), ' ', '')`;
}

// --- НОВАЯ getEntityKey с уникальным префиксом ---
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
// ---

// --- ОПРЕДЕЛЕНИЕ ТИПА ЗАПРОСА ---
function determineQueryType(query) {
    // Проверка на email (простая проверка)
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (emailRegex.test(query)) {
        return { type: 'email', value: query.toLowerCase() };
    }

    // Проверка на телефон (убираем все, кроме цифр)
    const cleanedPhone = cleanPhone(query);
    if (/^(7|8)?\d{10}$/.test(cleanedPhone)) {
        const normalizedForSearch = cleanedPhone.length === 10 ? '7' + cleanedPhone : cleanedPhone.replace(/^8/, '7');
        return { type: 'phone', value: normalizedForSearch };
    }

    // Проверка на OGRN (13 цифр)
    if (/^\d{13}$/.test(query)) {
        return { type: 'ogrn', value: query };
    }

    // Если не email, не телефон, не ОГРН, считаем это обычным текстовым запросом
    return { type: 'text', value: query };
}

function determineEntityType(connectedEntity) {
    let entityType = 'unknown';
    
    // Если UrFiz и fIP есть (не NULL), используем их для определения типа
    if (connectedEntity.UrFiz !== null && connectedEntity.UrFiz !== undefined) {
        if (connectedEntity.UrFiz === 1) {
            // Юрлицо
            if (connectedEntity.fIP === 1 || connectedEntity.fIP === true) {
                entityType = 'ip';
            } else {
                entityType = 'juridical';
            }
        } else if (connectedEntity.UrFiz === 2) {
            // Физлицо
            entityType = 'physical';
        }
    } else {
        // Если UrFiz нет (например, для таблицы contact), определяем по sourceTable
        switch(connectedEntity.sourceTable) {
            case 'contact':
                entityType = 'contact';
                break;
            case 'person_via_contact':
            case 'person_from_prevwork_via_contact':
            case 'employee':
            case 'employee_via_contact':
            case 'person_from_prevwork_phone':
                entityType = 'physical';
                break;
            case 'contragent':
            case 'contperson':
            case 'contperson_via_contact':
                if (connectedEntity.fIP === 1 || connectedEntity.fIP === true) {
                    entityType = 'ip';
                } else if (connectedEntity.INN && connectedEntity.INN.length === 10) {
                    entityType = 'juridical';
                } else if (connectedEntity.INN && connectedEntity.INN.length === 12) {
                    entityType = 'physical';
                }
                break;
            default:
                // Fallback
                if (connectedEntity.INN && connectedEntity.INN.length === 10) {
                    entityType = 'juridical';
                } else if (connectedEntity.INN && connectedEntity.INN.length === 12) {
                    if (connectedEntity.fIP === 1 || connectedEntity.fIP === true) {
                        entityType = 'ip';
                    } else {
                        entityType = 'physical';
                    }
                } else if (connectedEntity.NameShort || connectedEntity.NameFull) {
                    entityType = 'physical';
                }
        }
    }
    
    return entityType;
}

function normalizeEntity(entity) {
    if (!entity) return entity;
    // Нормализуем ИНН
    if (entity && entity.inn && !entity.INN) {
        entity.INN = entity.inn;
    }
    // Нормализуем OGRN
    if (entity && entity.ogrn && !entity.OGRN) {
        entity.OGRN = entity.ogrn;
    }
    // Нормализуем NameShort
    if (entity && entity.name_short && !entity.NameShort) {
        entity.NameShort = entity.name_short;
    }
    // Нормализуем NameFull
    if (entity && entity.name_full && !entity.NameFull) {
        entity.NameFull = entity.name_full;
    }
    // Нормализуем eMail
    if (entity && entity.email && !entity.eMail) {
        entity.eMail = entity.email;
    }
    // Нормализуем PhoneNum
    if (entity && entity.phone && !entity.PhoneNum) {
        entity.PhoneNum = entity.phone;
    }
    // Нормализуем AddressUr
    if (entity && entity.address_ur && !entity.AddressUr) {
        entity.AddressUr = entity.address_ur;
    }
    // Нормализуем AddressUFakt
    if (entity && entity.address_ufakt && !entity.AddressUFakt) {
        entity.AddressUFakt = entity.address_ufakt;
    }
    // Нормализуем UrFiz
    if (entity && entity.ur_fiz && !entity.UrFiz) {
        entity.UrFiz = entity.ur_fiz;
    }
    // Нормализуем fIP
    if (entity && entity.f_ip !== undefined && entity.fIP === undefined) {
        entity.fIP = entity.f_ip;
    }

    // Убедимся, что fIP - boolean, если оно есть
    if (entity && entity.fIP !== undefined) {
        entity.fIP = Boolean(entity.fIP);
    }
    // Убедимся, что UrFiz - число, если оно есть
    if (entity && entity.UrFiz !== undefined) {
        entity.UrFiz = Number(entity.UrFiz);
    }

    return entity;
}
// ---

export {
    determineEntityType,
    cleanPhone,
    getEntityKey,
    normalizePhoneSQL,
    determineEntityTypeSpec,
    determineQueryType, 
    normalizeEntity
};