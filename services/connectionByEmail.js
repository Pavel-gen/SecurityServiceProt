import sql from 'mssql'
import { determineEntityType, getEntityKey } from '../utils/helper.js';
import { buildEmailQuery } from '../queries/email.queries.js';

const EMAIL_SEARCH_TYPES = {
    CONTACT: 'contact',
    PERSON_VIA_CONTACT: 'person_via_contact',
    EMPLOYEE_VIA_CONTACT: 'employee_via_contact', 
    CONTPERSON_VIA_CONTACT: 'contperson_via_contact',
    PREVWORK_VIA_CONTACT: 'person_from_prevwork_via_contact',
    PERSON_FROM_PREVWORK_EMAIL: 'person_from_prevwork_email',
    CONTRAGENT: 'contragent',
    EMPLOYEE: 'employee',
    CONTPERSON: 'contperson'
};

async function findConnectionsByEmail(targetEntities) {
    console.log("Запуск findConnectionsByEmail с targetEntities: ", targetEntities);

    const { entitiesByKey, targetEmails } = prepareEmailSearchData(targetEntities);
    
    if (targetEmails.length === 0) {
        console.log("Нет email для поиска связей");
        return createEmptyConnectionsMap(entitiesByKey);
    }

    console.log("Начинаем поиск по email: ", targetEmails);

    const connectionsMap = createEmptyConnectionsMap(entitiesByKey);

    try {
        const emailResult = await executeEmailQuery(targetEmails);
        console.log("Найдено результатов по email:", emailResult.recordset.length);

        await processEmailResults(emailResult.recordset, entitiesByKey, connectionsMap, targetEmails);

    } catch (err) {
        console.error('Ошибка при поиске связей по email:', err);
        throw err;
    }

    console.log(`Итоговый размер connectionsMap: ${connectionsMap.size}`);
    
    // ОБНОВЛЕННОЕ ЛОГИРОВАНИЕ ДЛЯ НОВОЙ СТРУКТУРЫ
    // console.log("=== ДЕТАЛЬНЫЙ ПРОСМОТР CONNECTIONS MAP ===");
    // connectionsMap.forEach((connections, entityKey) => {
    //     console.log(`Сущность: ${entityKey}`);
    //     Object.entries(connections).forEach(([connectionKey, connectionGroup]) => {
    //         console.log(`  Группа связей: ${connectionKey}`);
    //         console.log(`  Связанная сущность:`, connectionGroup.entity?.NameShort || 'N/A');
    //         console.log(`  Количество связей: ${connectionGroup.connections?.length || 0}`);
            
    //         if (connectionGroup.connections && Array.isArray(connectionGroup.connections)) {
    //             connectionGroup.connections.forEach((connection, index) => {
    //                 console.log(`    Связь ${index + 1}:`);
    //                 console.log(`      Тип: ${connection.connectionType}`);
    //                 console.log(`      Статус: ${connection.connectionStatus}`);
    //                 console.log(`      Детали: ${connection.connectionDetails}`);
    //             });
    //         } else {
    //             console.log(`    ❌ connections не является массивом:`, connectionGroup.connections);
    //         }
    //     });
    // });

    return connectionsMap;
}

// --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ (ПРОСТЫЕ) ---

function prepareEmailSearchData(targetEntities) {
    const targetEmails = new Set();
    const entitiesByKey = new Map();

    targetEntities.forEach(entity => {
        const entityKey = getEntityKey(entity);
        if (!entityKey) return;

        const emails = getAllEmails(entity);
        emails.forEach(email => {
            if (email && email.trim() !== '') {
                targetEmails.add(email.toLowerCase().trim());
            }
        });

        if (emails.length > 0) {
            entitiesByKey.set(entityKey, entity);
        }
    });

    return {
        entitiesByKey,
        targetEmails: Array.from(targetEmails).filter(email => email)
    };
}

function getAllEmails(entity) {
    const emails = new Set();
    
    const emailFields = ['eMail', 'cpMail', 'fzMail', 'contactEmail', 'Contact'];
    
    emailFields.forEach(field => {
        if (entity[field]) {
            const fieldEmails = extractEmails(entity[field]);
            fieldEmails.forEach(email => emails.add(email.toLowerCase()));
        }
    });
    
    return Array.from(emails);
}

function extractEmails(emailString) {
    if (!emailString || emailString.trim() === '') return [];
    
    return emailString
        .toLowerCase()
        .split(/[;,]/)
        .map(email => email.trim())
        .filter(email => {
            const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
            return email && emailRegex.test(email);
        });
}

function createEmptyConnectionsMap(entitiesByKey) {
    const connectionsMap = new Map();
    entitiesByKey.forEach((entity, entityKey) => {
        connectionsMap.set(entityKey, {});
    });
    return connectionsMap;
}

async function executeEmailQuery(emailArray) {
    const query = buildEmailQuery(emailArray);
    const request = new sql.Request();
    
    emailArray.forEach((email, index) => {
        request.input(`email${index}`, sql.VarChar, email);
    });
    
    return await request.query(query);
}

async function processEmailResults(records, entitiesByKey, connectionsMap, targetEmails) {
    // ПРОСТАЯ ГРУППИРОВКА ПО EMAIL
    const connectionsByEmail = new Map();
    
    // СОБИРАЕМ ВСЕ СУЩНОСТИ КОТОРЫЕ ДОЛЖНЫ ИМЕТЬ СВЯЗИ
    const allEntities = new Map(entitiesByKey);
    
    records.forEach(row => {
        const connectionInfo = createEmailConnection(row);
        const foundEmail = row.contactEmail?.toLowerCase();
        
        if (!foundEmail) return;

        const foundEmailsList = foundEmail.split(';').map(email => email.trim()).filter(email => email);
        const hasIntersection = foundEmailsList.some(email => targetEmails.includes(email));
        
        if (!hasIntersection) return;

        // ДОБАВЛЯЕМ НАЙДЕННЫЕ СУЩНОСТИ
        const connectedEntity = connectionInfo.connectedEntity;
        const connectedEntityKey = getEntityKey(connectedEntity);
        if (connectedEntityKey && !allEntities.has(connectedEntityKey)) {
            allEntities.set(connectedEntityKey, connectedEntity);
        }

        // ГРУППИРУЕМ ПО КАЖДОМУ EMAIL
        foundEmailsList.forEach(singleEmail => {
            if (!connectionsByEmail.has(singleEmail)) {
                connectionsByEmail.set(singleEmail, []);
            }
            
            // ДОБАВЛЯЕМ ТОЛЬКО УНИКАЛЬНЫЕ СВЯЗИ
            const existingConnections = connectionsByEmail.get(singleEmail);
            const isDuplicate = existingConnections.some(conn => 
                conn.connectionDetails === connectionInfo.connectionDetails
            );
            
            if (!isDuplicate) {
                connectionsByEmail.get(singleEmail).push(connectionInfo);
            }
        });
    });

    console.log(`📊 Найдено email с связями: ${Array.from(connectionsByEmail.keys()).join(', ')}`);
    console.log(`📊 Всего сущностей для связей: ${allEntities.size}`);

    // ДОБАВЛЯЕМ СВЯЗИ ДЛЯ ВСЕХ СУЩНОСТЕЙ
    allEntities.forEach((targetEntity, targetEntityKey) => {
        const targetEmailsList = getAllEmails(targetEntity);
        
        if (!connectionsMap.has(targetEntityKey)) {
            connectionsMap.set(targetEntityKey, {});
        }

        // ДОБАВЛЯЕМ СВЯЗИ ПО КАЖДОМУ EMAIL
        connectionsByEmail.forEach((connections, email) => {
            if (targetEmailsList.includes(email)) {
                connectionsMap.get(targetEntityKey)[email] = {
                    connections: connections
                };
                console.log(`✅ Добавлены связи для ${targetEntityKey} по email: ${email}`);
            }
        });
    });
}

function createEmailConnection(row) {
    const { connectionType, connectionStatus } = determineEmailConnectionInfo(row);
    
    const connectedEntity = createFullEntityFromEmailRow(row);
    
    return {
        connectedEntity: connectedEntity,
        connectionType: connectionType,
        connectionStatus: connectionStatus,
        connectionDetails: buildEmailConnectionDetails(row, connectionStatus)
    };
}

function createFullEntityFromEmailRow(row) {
    const entity = {
        // Основные поля
        INN: row.contactINN,
        NameShort: row.contactNameShort,
        NameFull: row.contactNameFull,
        sourceTable: row.sourceTable,
        source: 'local',
        baseName: row.baseName,
        eMail: row.contactEmail,
        UNID: row.contactUNID,
        fzUID: row.fzUID,
        cpUID: row.cpUID,
        PersonUNID: row.PersonUNID,
        UrFiz: row.UrFiz,
        fIP: row.fIP,
        
        // ДОПОЛНИТЕЛЬНЫЕ ПОЛЯ ИЗ SQL
        prevWorkCaption: row.prevWorkCaption,
        WorkPeriod: row.WorkPeriod,
        relatedPersonUNID: row.relatedPersonUNID,
        
        // ВАЖНО: Добавляем поля для ИНН поиска
        phOrgINN: row.phOrgINN,  // ИНН организации для сотрудников
        fzINN: row.fzINN,        // Личный ИНН сотрудника
        conINN: row.conINN       // ИНН организации для контактных лиц
    };
    
    entity.type = determineEntityType(entity);
    
    return entity;
}

function determineEmailConnectionInfo(row) {
    const connectionMap = {
        [EMAIL_SEARCH_TYPES.CONTRAGENT]: { connectionType: 'email_match', connectionStatus: 'organization_match' },
        [EMAIL_SEARCH_TYPES.EMPLOYEE]: { connectionType: 'email_match', connectionStatus: 'employee_match' },
        [EMAIL_SEARCH_TYPES.CONTPERSON]: { connectionType: 'email_match', connectionStatus: 'contact_person_match' },
        [EMAIL_SEARCH_TYPES.PERSON_VIA_CONTACT]: { connectionType: 'email_match', connectionStatus: 'person_match_via_contact' },
        [EMAIL_SEARCH_TYPES.EMPLOYEE_VIA_CONTACT]: { connectionType: 'email_match', connectionStatus: 'employee_match_via_contact' },
        [EMAIL_SEARCH_TYPES.CONTPERSON_VIA_CONTACT]: { connectionType: 'email_match', connectionStatus: 'contact_person_match_via_contact' },
        [EMAIL_SEARCH_TYPES.PREVWORK_VIA_CONTACT]: { connectionType: 'email_match', connectionStatus: 'prevwork_match' },
        [EMAIL_SEARCH_TYPES.CONTACT]: { connectionType: 'email_match', connectionStatus: 'contact_found' },
        [EMAIL_SEARCH_TYPES.PERSON_FROM_PREVWORK_EMAIL]: { connectionType: 'email_match', connectionStatus: 'person_match_from_prevwork_email' }
    };
    
    return connectionMap[row.sourceTable] || { connectionType: 'email_match', connectionStatus: 'unknown_status' };
}

function buildEmailConnectionDetails(row, connectionStatus) {
    let details = `Совпадение по email: ${row.contactEmail}, таблица: ${row.sourceTable}, статус: ${connectionStatus}`;
    
    // ДОБАВЛЯЕМ ИНФОРМАЦИЮ О МЕСТЕ РАБОТЫ ЕСЛИ ЕСТЬ
    if (row.prevWorkCaption) {
        details += `, место работы: ${row.prevWorkCaption}`;
    }
    if (row.WorkPeriod) {
        details += `, период: ${row.WorkPeriod}`;
    }
    
    return details;
}

function addEmailConnectionToTargets(entitiesByKey, connectionsMap, searchEmail, connectionInfo) {
    entitiesByKey.forEach((targetEntity, targetEntityKey) => {
        const targetEmailsList = getAllEmails(targetEntity);
        
        const hasEmailIntersection = targetEmailsList.includes(searchEmail);
        
        if (!hasEmailIntersection) return;

        // Проверка на самосвязь
        const connectedEntity = connectionInfo.connectedEntity;
        const connectedKey = getEntityKey(connectedEntity);
        const targetKey = getEntityKey(targetEntity);
        
        if (targetKey === connectedKey) return;

        if (!connectionsMap.has(targetEntityKey)) {
            connectionsMap.set(targetEntityKey, {});
        }
        
        if (!connectionsMap.get(targetEntityKey)[searchEmail]) {
            connectionsMap.get(targetEntityKey)[searchEmail] = [];
        }
        
        connectionsMap.get(targetEntityKey)[searchEmail].push(connectionInfo);
    });
}

export { findConnectionsByEmail };