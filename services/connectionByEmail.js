import sql from 'mssql'
import { determineEntityType, getEntityKey } from '../utils/helper.js';
import { buildEmailQuery } from '../queries/email.queries.js';

// Конфигурация для email поиска
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

const EMAIL_CONNECTION_STATUS = {
    ORGANIZATION_MATCH: 'organization_match',
    EMPLOYEE_MATCH: 'employee_match',
    CONTACT_PERSON_MATCH: 'contact_person_match',
    PREVWORK_MATCH: 'prevwork_match',
    PERSON_MATCH_VIA_CONTACT: 'person_match_via_contact',
    EMPLOYEE_MATCH_VIA_CONTACT: 'employee_match_via_contact',
    CONTACT_PERSON_MATCH_VIA_CONTACT: 'contact_person_match_via_contact',
    CONTACT_FOUND: 'contact_found',
    PERSON_MATCH_FROM_PREVWORK_EMAIL: 'person_match_from_prevwork_email'
};

async function findConnectionsByEmail(targetEntities) {
    console.log("Запуск findConnectionsByEmail с targetEntities: ", targetEntities);

    const { entitiesByKey, targetEmails } = prepareEmailSearchData(targetEntities);
    
    if (targetEmails.length === 0) {
        console.log("Нет email для поиска связей");
        return createEmptyConnectionsMap(entitiesByKey);
    }

    // СОЗДАЕМ РАСШИРЕННЫЙ connectionsMap КОТОРЫЙ БУДЕМ ПОПОЛНЯТЬ
    const connectionsMap = createEmptyConnectionsMap(entitiesByKey);
    const foundEntitiesMap = new Map(); // Для отслеживания уже найденных сущностей

    try {
        const emailResult = await executeEmailQuery(targetEmails);
        console.log("Найдено результатов по email:", emailResult.recordset.length);

        // ОБРАБАТЫВАЕМ РЕЗУЛЬТАТЫ И ДОБАВЛЯЕМ НАЙДЕННЫЕ СУЩНОСТИ В MAP
        await processEmailResults(
            emailResult.recordset, 
            entitiesByKey, 
            connectionsMap, 
            targetEmails,
            foundEntitiesMap
        );

    } catch (err) {
        console.error('Ошибка при поиске связей по email:', err);
        throw err;
    }

    console.log(`Итоговый размер connectionsMap: ${connectionsMap.size}`);
    console.log("Вернувшийся connnectionMap", connectionsMap);
    return connectionsMap;
}
// --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---

function prepareEmailSearchData(targetEntities) {
    const targetEmails = new Set();
    const entitiesByKey = new Map();

    const emailFieldConfig = [
        { condition: (entity) => entity.type === 'contact' && entity.ContactType === 'E-Mail', field: 'Contact' },
        { condition: (entity) => entity.eMail, field: 'eMail' },
        { condition: (entity) => entity.cpMail, field: 'cpMail' },
        { condition: (entity) => entity.contactEmail, field: 'contactEmail' },
        { condition: (entity) => entity.EMail, field: 'EMail' },
        { condition: (entity) => entity.fzMail, field: 'fzMail' }
    ];

    targetEntities.forEach(entity => {
        const entityKey = getEntityKey(entity);
        if (!entityKey) return;

        const config = emailFieldConfig.find(config => config.condition(entity));
        if (!config) return;

        const emailFieldValue = entity[config.field];
        const emails = extractEmails(emailFieldValue);

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

function extractEmails(emailString) {
    if (!emailString || emailString.trim() === '') return [];
    
    const result = emailString
        .toLowerCase()
        .split(/[;,]/)
        .map(email => email.trim())
        .filter(email => {
            const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
            return email && emailRegex.test(email);
        });
    
    console.log(`extractEmails из "${emailString}":`, result);
    return result;
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

async function processEmailResults(records, entitiesByKey, connectionsMap, targetEmails, foundEntitiesMap) {
    const allConnections = new Map();
    
    // 1. Сначала собираем все основные связи
    records.forEach(row => {
        const connectionInfo = createEmailConnection(row);
        const foundEmail = row.contactEmail?.toLowerCase();
        
        if (!foundEmail) return;

        const foundEmailsList = foundEmail.split(';').map(email => email.trim()).filter(email => email);
        const hasIntersection = foundEmailsList.some(email => targetEmails.includes(email));
        
        if (!hasIntersection) return;

        foundEmailsList.forEach(singleEmail => {
            // Связи для исходных сущностей
            addConnectionToEntity(entitiesByKey, allConnections, singleEmail, connectionInfo);
            
            // Связи для найденных сущностей
            const foundEntity = connectionInfo.connectedEntity;
            const foundEntityKey = getEntityKey(foundEntity);
            if (foundEntityKey) {
                if (!allConnections.has(foundEntityKey)) {
                    allConnections.set(foundEntityKey, {});
                }
                if (!allConnections.get(foundEntityKey)[singleEmail]) {
                    allConnections.get(foundEntityKey)[singleEmail] = [];
                }
                allConnections.get(foundEntityKey)[singleEmail].push(connectionInfo);
            }
        });
    });

    // 2. ОТДЕЛЬНО ДОБАВЛЯЕМ КОНТАКТЫ КАК СВЯЗИ
    addContactConnections(records, allConnections, targetEmails);

    // 3. Заполняем окончательный connectionsMap
    entitiesByKey.forEach((entity, entityKey) => {
        if (!connectionsMap.has(entityKey)) {
            connectionsMap.set(entityKey, allConnections.get(entityKey) || {});
        }
    });

    allConnections.forEach((connections, entityKey) => {
        if (!connectionsMap.has(entityKey)) {
            connectionsMap.set(entityKey, connections);
            console.log(`✅ Добавлена новая сущность со связями: ${entityKey}`);
        }
    });
}

function addConnectionToEntity(entitiesByKey, allConnections, searchEmail, connectionInfo) {
    const connectedEntity = connectionInfo.connectedEntity;
    const connectedKey = getEntityKey(connectedEntity);
    
    entitiesByKey.forEach((targetEntity, targetEntityKey) => {
        const targetEmailsList = getAllEmails(targetEntity);
        
        const hasEmailIntersection = targetEmailsList.includes(searchEmail);
        
        if (!hasEmailIntersection) return;

        // УСИЛЕННАЯ ПРОВЕРКА НА САМОСВЯЗЬ
        const targetKey = getEntityKey(targetEntity);
        
        // Пропускаем если это одна и та же сущность
        if (targetKey === connectedKey) {
            console.log(`⏭️ Пропускаем самосвязь: ${targetKey}`);
            return;
        }
        
        // Пропускаем если это контакт ссылающийся на самого себя
        if (targetEntity.type === 'contact' && connectedEntity.type === 'contact' && 
            targetEntity.PersonUNID === connectedEntity.PersonUNID) {
            console.log(`⏭️ Пропускаем самосвязь контакта: ${targetKey}`);
            return;
        }

        if (!allConnections.has(targetEntityKey)) {
            allConnections.set(targetEntityKey, {});
        }
        
        if (!allConnections.get(targetEntityKey)[searchEmail]) {
            allConnections.get(targetEntityKey)[searchEmail] = [];
        }
        
        allConnections.get(targetEntityKey)[searchEmail].push(connectionInfo);
    });
}

function addContactConnections(records, allConnections, targetEmails) {
    const contactConnections = new Map();
    
    records.forEach(row => {
        // ЕСЛИ ЭТО КОНТАКТ - ДОБАВЛЯЕМ ЕГО КАК СВЯЗАННУЮ СУЩНОСТЬ
        if (row.sourceTable === 'contact' || row.sourceTable === 'CF_Contacts_test') {
            const connectionInfo = createEmailConnection(row);
            const foundEmail = row.contactEmail?.toLowerCase();
            
            if (!foundEmail) return;

            const foundEmailsList = foundEmail.split(';').map(email => email.trim()).filter(email => email);
            const hasIntersection = foundEmailsList.some(email => targetEmails.includes(email));
            
            if (!hasIntersection) return;

            const contactEntity = connectionInfo.connectedEntity;
            const contactKey = getEntityKey(contactEntity);
            
            if (!contactKey) return;

            // ДОБАВЛЯЕМ КОНТАКТ КАК СВЯЗАННУЮ СУЩНОСТЬ ДЛЯ ВСЕХ СУЩНОСТЕЙ С ЭТИМ EMAIL
            foundEmailsList.forEach(singleEmail => {
                // Ищем все сущности у которых есть этот email (кроме самого контакта)
                allConnections.forEach((connections, entityKey) => {
                    const entity = getEntityFromConnections(allConnections, entityKey);
                    if (entity && entity.type !== 'contact') {
                        const entityEmails = getAllEmails(entity);
                        if (entityEmails.includes(singleEmail) && entityKey !== contactKey) {
                            if (!allConnections.has(entityKey)) {
                                allConnections.set(entityKey, {});
                            }
                            if (!allConnections.get(entityKey)[singleEmail]) {
                                allConnections.get(entityKey)[singleEmail] = [];
                            }
                            
                            // ДОБАВЛЯЕМ КОНТАКТ КАК СВЯЗЬ
                            allConnections.get(entityKey)[singleEmail].push(connectionInfo);
                            console.log(`✅ Добавлен контакт как связь для ${entityKey}`);
                        }
                    }
                });
            });
        }
    });
}

function getEntityFromConnections(allConnections, entityKey) {
    const connections = allConnections.get(entityKey);
    if (!connections) return null;
    
    // Берем первую connectedEntity из любой связи
    for (const connectionList of Object.values(connections)) {
        if (connectionList && connectionList.length > 0) {
            return connectionList[0].connectedEntity;
        }
    }
    return null;
}


// ГЛАВНОЕ ИЗМЕНЕНИЕ - создаем полноценную connectedEntity как в INN поиске
function createEmailConnection(row) {
    const { connectionType, connectionStatus } = determineEmailConnectionInfo(row);
    
    // СОЗДАЕМ ПОЛНОЦЕННУЮ СУЩНОСТЬ
    const connectedEntity = createFullEntityFromEmailRow(row);
    
    const connection = {
        connectedEntity: connectedEntity,
        connectionType: connectionType,
        connectionStatus: connectionStatus,
        connectionDetails: buildEmailConnectionDetails(row, connectionStatus)
    };
    
    return connection;
}

// КРИТИЧЕСКИ ВАЖНАЯ ФУНКЦИЯ - создает сущность с правильной структурой
function createFullEntityFromEmailRow(row) {
    const entity = {
        // Основные поля
        INN: row.contactINN,
        OGRN: row.OGRN,
        NameShort: row.contactNameShort,
        NameFull: row.contactNameFull,
        sourceTable: row.sourceTable,
        source: 'local',
        baseName: row.baseName,
        eMail: row.contactEmail,
        
        // Все возможные ID
        UNID: row.contactUNID,
        fzUID: row.fzUID,
        cpUID: row.cpUID,
        PersonUNID: row.PersonUNID,
        
        // Поля для определения типа
        UrFiz: row.UrFiz,
        fIP: row.fIP,
        fSZ: row.fSZ
    };
    
    // Определяем тип сущности
    entity.type = determineEntityType(entity);
    
    return entity;
}

function determineEmailConnectionInfo(row) {
    const connectionMap = {
        [EMAIL_SEARCH_TYPES.CONTRAGENT]: { 
            connectionType: 'email_match', 
            connectionStatus: EMAIL_CONNECTION_STATUS.ORGANIZATION_MATCH 
        },
        [EMAIL_SEARCH_TYPES.EMPLOYEE]: { 
            connectionType: 'email_match', 
            connectionStatus: EMAIL_CONNECTION_STATUS.EMPLOYEE_MATCH 
        },
        [EMAIL_SEARCH_TYPES.CONTPERSON]: { 
            connectionType: 'email_match', 
            connectionStatus: EMAIL_CONNECTION_STATUS.CONTACT_PERSON_MATCH 
        },
        [EMAIL_SEARCH_TYPES.PERSON_VIA_CONTACT]: { 
            connectionType: 'email_match', 
            connectionStatus: EMAIL_CONNECTION_STATUS.PERSON_MATCH_VIA_CONTACT 
        },
        [EMAIL_SEARCH_TYPES.EMPLOYEE_VIA_CONTACT]: { 
            connectionType: 'email_match', 
            connectionStatus: EMAIL_CONNECTION_STATUS.EMPLOYEE_MATCH_VIA_CONTACT 
        },
        [EMAIL_SEARCH_TYPES.CONTPERSON_VIA_CONTACT]: { 
            connectionType: 'email_match', 
            connectionStatus: EMAIL_CONNECTION_STATUS.CONTACT_PERSON_MATCH_VIA_CONTACT 
        },
        [EMAIL_SEARCH_TYPES.PREVWORK_VIA_CONTACT]: { 
            connectionType: 'email_match', 
            connectionStatus: EMAIL_CONNECTION_STATUS.PREVWORK_MATCH 
        },
        [EMAIL_SEARCH_TYPES.CONTACT]: { 
            connectionType: 'email_match', 
            connectionStatus: EMAIL_CONNECTION_STATUS.CONTACT_FOUND 
        },
        [EMAIL_SEARCH_TYPES.PERSON_FROM_PREVWORK_EMAIL]: { 
            connectionType: 'email_match', 
            connectionStatus: EMAIL_CONNECTION_STATUS.PERSON_MATCH_FROM_PREVWORK_EMAIL 
        }
    };
    
    return connectionMap[row.sourceTable] || { 
        connectionType: 'email_match', 
        connectionStatus: 'unknown_status' 
    };
}

function extractEmployeeInfoFromEmail(row) {
    return {
        fzFIO: row.fzFIO,
        phFunction: row.phFunction,
        phEventType: row.phEventType,
        phDate: row.phDate ? new Date(row.phDate).toLocaleDateString() : null,
        phDep: row.phDep
    };
}

function buildEmailConnectionDetails(row, connectionStatus) {
    const sourceInfo = row.sourceTable === EMAIL_SEARCH_TYPES.CONTACT ? 
        'найден в контактах' : 
        `найден в таблице ${row.sourceTable}`;
        
    return `Совпадение по email: ${row.contactEmail}, ${sourceInfo}, статус: ${connectionStatus}`;
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
        
        if (targetKey === connectedKey) {
            console.log('Пропускаем самосвязь:', targetKey);
            return;
        }

        if (!connectionsMap.has(targetEntityKey)) {
            connectionsMap.set(targetEntityKey, {});
        }
        
        if (!connectionsMap.get(targetEntityKey)[searchEmail]) {
            connectionsMap.get(targetEntityKey)[searchEmail] = [];
        }
        
        connectionsMap.get(targetEntityKey)[searchEmail].push(connectionInfo);
    });
}

// Аналог getAllINNs но для email
function getAllEmails(entity) {
    const emails = new Set();
    
    const emailFields = ['eMail', 'cpMail', 'fzMail', 'contactEmail', 'Contact'];
    
    emailFields.forEach(field => {
        if (entity[field]) {
            const fieldEmails = extractEmails(entity[field]);
            fieldEmails.forEach(email => emails.add(email.toLowerCase()));
        }
    });
    
    console.log(`getAllEmails для ${getEntityKey(entity)}:`, Array.from(emails), entity.type);
    return Array.from(emails);
}
export { findConnectionsByEmail };