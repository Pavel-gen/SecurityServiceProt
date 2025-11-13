import sql from 'mssql'
import { determineEntityType, getEntityKey } from '../utils/helper.js';
import { buildEmailQuery } from '../queries/email.queries.js';

// Конфигурация для email поиска
const EMAIL_SEARCH_TYPES = {
    CONTACT: 'contact',
    PERSON_VIA_CONTACT: 'person_via_contact',
    EMPLOYEE_VIA_CONTACT: 'employee_via_contact',
    CONTPERSON_VIA_CONTACT: 'contperson_via_contact',
    PREVWORK_VIA_CONTACT: 'prevwork_via_contact',
    PERSON_FROM_PREVWORK: 'person_from_prevwork_via_contact',
    PERSON_FROM_PREVWORK_EMAIL: 'person_from_prevwork_email',
    CONTRAGENT: 'contragent',
    EMPLOYEE: 'employee',
    CONTPERSON: 'contperson',
    PREVWORK: 'prevwork'
};

const EMAIL_CONNECTION_STATUS = {
    ORGANIZATION_MATCH: 'organization_match',
    EMPLOYEE_MATCH: 'employee_match',
    CONTACT_PERSON_MATCH: 'contact_person_match',
    PREVWORK_MATCH: 'prevwork_match',
    PERSON_MATCH_VIA_CONTACT: 'person_match_via_contact',
    EMPLOYEE_MATCH_VIA_CONTACT: 'employee_match_via_contact',
    CONTACT_PERSON_MATCH_VIA_CONTACT: 'contact_person_match_via_contact',
    PREVWORK_MATCH_VIA_CONTACT: 'prevwork_match_via_contact',
    CONTACT_FOUND: 'contact_found',
    PERSON_MATCH_FROM_PREVWORK_EMAIL: 'person_match_from_prevwork_email',
    PERSON_MATCH_VIA_CONTACT_FROM_PREVWORK: 'person_match_via_contact_from_prevwork'
};

async function findConnectionsByEmail(targetEntities) {
    console.log("Запуск findConnectionsByEmail");

    const { entitiesByKey, targetEmails } = prepareEmailSearchData(targetEntities);
    
    if (targetEmails.length === 0) {
        console.log("Нет email для поиска связей");
        return createEmptyConnectionsMap(entitiesByKey);
    }

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
    return connectionsMap;
}

// --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---

function prepareEmailSearchData(targetEntities) {
    const targetEmails = new Set();
    const entitiesByKey = new Map();

    targetEntities.forEach(entity => {
        const entityKey = getEntityKey(entity);
        if (entityKey && entity.eMail && entity.eMail.trim() !== '') {
            const emails = entity.eMail.toLowerCase().split(';').map(email => email.trim()).filter(email => email);
            emails.forEach(email => targetEmails.add(email));
            entitiesByKey.set(entityKey, entity);
        }
    });

    return {
        entitiesByKey,
        targetEmails: Array.from(targetEmails).filter(email => email)
    };
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
    records.forEach(row => {
        const connectionInfo = createEmailConnection(row);
        const foundEmail = row.contactEmail?.toLowerCase();
        
        if (!foundEmail) {
            return;
        }

        // Проверяем пересечение email, а не точное совпадение
        const foundEmailsList = foundEmail.split(';').map(email => email.trim()).filter(email => email);
        const hasIntersection = foundEmailsList.some(email => targetEmails.includes(email));
        
        if (!hasIntersection) {
            return;
        }

        console.log(connectionsMap);

        // Передаем КАЖДЫЙ email из foundEmailsList для создания связей
        foundEmailsList.forEach(singleEmail => {
            addEmailConnectionToTargets(entitiesByKey, connectionsMap, singleEmail, connectionInfo);
        });
    });
}

function createEmailConnection(row) {
    const { connectionType, connectionStatus } = determineEmailConnectionInfo(row);
    const connectedName = buildConnectedName(row);
    
    // СОЗДАЕМ connectedEntity с правильной структурой для getEntityKey
    const connectedEntity = {
        // Основные поля
        INN: row.contactINN,
        NameShort: connectedName,
        NameFull: row.contactNameFull,
        type: determineEmailEntityType(row),
        sourceTable: row.sourceTable,
        source: 'local',
        baseName: row.baseName,
        
        // ВСЕ возможные ID поля - КРИТИЧЕСКИ ВАЖНО!
        UNID: row.contactUNID, // для контрагентов
        fzUID: row.fzUID,      // для сотрудников  
        cpUID: row.cpUID,      // для контактных лиц
        PersonUNID: row.PersonUNID, // для персон
        contactUNID: row.contactUNID, // общее поле
        
        // Дополнительные поля
        prevWorkCaption: row.prevWorkCaption
    };

    // console.log('=== DEBUG createEmailConnection ===');
    // console.log('sourceTable:', row.sourceTable);
    // console.log('connectedEntity для ключа:', {
    //     sourceTable: connectedEntity.sourceTable,
    //     UNID: connectedEntity.UNID,
    //     fzUID: connectedEntity.fzUID,
    //     cpUID: connectedEntity.cpUID,
    //     PersonUNID: connectedEntity.PersonUNID
    // });
    // console.log('Сгенерированный ключ:', getEntityKey(connectedEntity));

    return {
        connectedEntity: connectedEntity,
        connectionType: connectionType,
        connectionStatus: connectionStatus,
        connectionDetails: buildEmailConnectionDetails(row, connectionStatus)
    };
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
        [EMAIL_SEARCH_TYPES.PREVWORK]: { 
            connectionType: 'email_match', 
            connectionStatus: EMAIL_CONNECTION_STATUS.PREVWORK_MATCH 
        },
        [EMAIL_SEARCH_TYPES.PERSON_VIA_CONTACT]: { 
            connectionType: 'person_unid_via_email_match', 
            connectionStatus: EMAIL_CONNECTION_STATUS.PERSON_MATCH_VIA_CONTACT 
        },
        [EMAIL_SEARCH_TYPES.EMPLOYEE_VIA_CONTACT]: { 
            connectionType: 'person_unid_via_email_match', 
            connectionStatus: EMAIL_CONNECTION_STATUS.EMPLOYEE_MATCH_VIA_CONTACT 
        },
        [EMAIL_SEARCH_TYPES.CONTPERSON_VIA_CONTACT]: { 
            connectionType: 'person_unid_via_email_match', 
            connectionStatus: EMAIL_CONNECTION_STATUS.CONTACT_PERSON_MATCH_VIA_CONTACT 
        },
        [EMAIL_SEARCH_TYPES.PREVWORK_VIA_CONTACT]: { 
            connectionType: 'person_unid_via_email_match', 
            connectionStatus: EMAIL_CONNECTION_STATUS.PREVWORK_MATCH_VIA_CONTACT 
        },
        [EMAIL_SEARCH_TYPES.CONTACT]: { 
            connectionType: 'email_match', 
            connectionStatus: EMAIL_CONNECTION_STATUS.CONTACT_FOUND 
        },
        [EMAIL_SEARCH_TYPES.PERSON_FROM_PREVWORK_EMAIL]: { 
            connectionType: 'person_unid_via_email_match', 
            connectionStatus: EMAIL_CONNECTION_STATUS.PERSON_MATCH_FROM_PREVWORK_EMAIL 
        },
        [EMAIL_SEARCH_TYPES.PERSON_FROM_PREVWORK]: { 
            connectionType: 'person_unid_via_email_match', 
            connectionStatus: EMAIL_CONNECTION_STATUS.PERSON_MATCH_VIA_CONTACT_FROM_PREVWORK 
        }
    };
    
    return connectionMap[row.sourceTable] || { 
        connectionType: 'email_match', 
        connectionStatus: 'unknown_status' 
    };
}

function determineEmailEntityType(row) {
    const typeMap = {
        [EMAIL_SEARCH_TYPES.CONTRAGENT]: determineEntityType(row.UrFiz, row.fIP),
        [EMAIL_SEARCH_TYPES.EMPLOYEE]: 'physical',
        [EMAIL_SEARCH_TYPES.CONTPERSON]: 'physical',
        [EMAIL_SEARCH_TYPES.PREVWORK]: 'legal',
        [EMAIL_SEARCH_TYPES.PERSON_VIA_CONTACT]: 'physical',
        [EMAIL_SEARCH_TYPES.EMPLOYEE_VIA_CONTACT]: 'physical',
        [EMAIL_SEARCH_TYPES.CONTPERSON_VIA_CONTACT]: 'physical',
        [EMAIL_SEARCH_TYPES.PREVWORK_VIA_CONTACT]: 'legal',
        [EMAIL_SEARCH_TYPES.CONTACT]: 'contact',
        [EMAIL_SEARCH_TYPES.PERSON_FROM_PREVWORK_EMAIL]: 'physical',
        [EMAIL_SEARCH_TYPES.PERSON_FROM_PREVWORK]: 'physical'
    };
    
    return typeMap[row.sourceTable] || 'unknown';
}

function buildConnectedName(row) {
    if (row.sourceTable === EMAIL_SEARCH_TYPES.CONTACT) {
        return row.contactEmail;
    }
    return row.contactNameShort || row.contactNameFull || row.contactEmail || 'N/A';
}

function buildEmailConnectionDetails(row, connectionStatus) {
    const indirectMarker = row.relatedPersonUNID ? ' (косвенно через PersonUNID)' : '';
    return `Совпадение по email: ${row.contactEmail}, найдено в таблице ${row.sourceTable}${indirectMarker}, статус: ${connectionStatus}`;
}

function addEmailConnectionToTargets(entitiesByKey, connectionsMap, searchEmail, connectionInfo) {
    entitiesByKey.forEach((targetEntity, targetEntityKey) => {
        const targetEmailsList = (targetEntity.eMail || '').toLowerCase().split(';').map(email => email.trim()).filter(email => email);
        
        // ПРАВИЛЬНАЯ проверка пересечения email
        const hasEmailIntersection = targetEmailsList.includes(searchEmail) || 
                                   targetEmailsList.some(targetEmail => targetEmail.includes(searchEmail));

        if (!hasEmailIntersection) {
            return;
        }

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

export { findConnectionsByEmail };