import sql from 'mssql'
import { determineEntityType, getEntityKey } from '../utils/helper.js';
import { buildINNQuery } from '../queries/inn.queries.js';

// --- ОСНОВНАЯ ФУНКЦИЯ: Поиск связей по ИНН ---
async function findConnectionsByINN(targetEntities) {
    console.log("Запуск findConnectionsByINN");

    const { entitiesByKey, targetINNs } = prepareSearchData(targetEntities);
    
    if (targetINNs.length === 0) {
        console.log("Нет ИНН для поиска связей");
        return createEmptyConnectionsMap(entitiesByKey);
    }

    const connectionsMap = createEmptyConnectionsMap(entitiesByKey);

    try {
        const innResult = await executeINNQuery(targetINNs);
        console.log("Найдено результатов по ИНН:", innResult.recordset.length);

        await processSearchResults(innResult.recordset, entitiesByKey, connectionsMap);

    } catch (err) {
        console.error('Ошибка при поиске связей по ИНН:', err);
        throw err;
    }

    console.log(`Итоговый размер connectionsMap: ${connectionsMap.size}`);
    return connectionsMap;
}

// --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---

function prepareSearchData(targetEntities) {
    const targetINNs = new Set();
    const entitiesByKey = new Map();

    targetEntities.forEach(entity => {
        if (entity.type === 'prevwork') {
            console.log(`Пропускаем prevwork сущность с ИНН ${entity.INN}`);
            return;
        }

        const entityKey = getEntityKey(entity);
        if (entityKey && entity.INN?.trim()) {
            targetINNs.add(entity.INN);
            entitiesByKey.set(entityKey, entity);
        }
    });

    return {
        entitiesByKey,
        targetINNs: Array.from(targetINNs).filter(inn => inn)
    };
}

function createEmptyConnectionsMap(entitiesByKey) {
    const connectionsMap = new Map();
    entitiesByKey.forEach((entity, entityKey) => {
        connectionsMap.set(entityKey, {});
    });
    return connectionsMap;
}

async function executeINNQuery(innArray) {
    const query = buildINNQuery(innArray);
    const request = new sql.Request();
    
    innArray.forEach((inn, index) => {
        request.input(`inn${index}`, sql.VarChar, inn);
    });
    
    return await request.query(query);
}

async function processSearchResults(records, entitiesByKey, connectionsMap) {
    // Группируем записи по типам для обработки
    const recordsByType = groupRecordsByType(records);
    
    // Обрабатываем записи о предыдущих местах работы
    if (recordsByType.prevwork.length > 0) {
        await processPrevWorkRecords(recordsByType.prevwork, entitiesByKey, connectionsMap);
    }
    
    // Обрабатываем прямые совпадения по ИНН
    if (recordsByType.direct.length > 0) {
        processDirectMatches(recordsByType.direct, entitiesByKey, connectionsMap);
    }
    
    // Обрабатываем остальные записи
    if (recordsByType.other.length > 0) {
        processOtherRecords(recordsByType.other, entitiesByKey, connectionsMap);
    }
}

function groupRecordsByType(records) {
    const groups = {
        prevwork: [],    // prevwork_by_org_inn
        direct: [],      // person_direct_inn_match
        other: []        // все остальные
    };
    
    records.forEach(record => {
        if (record.sourceTable === 'prevwork_by_org_inn') {
            groups.prevwork.push(record);
        } else if (record.sourceTable === 'person_direct_inn_match') {
            groups.direct.push(record);
        } else {
            groups.other.push(record);
        }
    });
    
    return groups;
}

// --- ОБРАБОТКА РАЗЛИЧНЫХ ТИПОВ ЗАПИСЕЙ ---

async function processPrevWorkRecords(records, entitiesByKey, connectionsMap) {
    const personUNIDs = [...new Set(records.map(r => r.PersonUNID).filter(Boolean))];
    const personDetailsMap = await findPersonDetailsByUNID(personUNIDs);
    
    for (const record of records) {
        await processSinglePrevWorkRecord(record, entitiesByKey, connectionsMap, personDetailsMap);
    }
}

async function processSinglePrevWorkRecord(record, entitiesByKey, connectionsMap, personDetailsMap) {
    const { relatedOrgINN, personUNID } = record;
    
    if (!personUNID) return;
    
    const personDetails = personDetailsMap.get(personUNID);
    const connectionInfo = createPrevWorkConnection(record, personDetails);
    
    // Находим все целевые сущности с этим ИНН организации
    findAndAddConnections(entitiesByKey, connectionsMap, relatedOrgINN, connectionInfo);
}

function createPrevWorkConnection(record, personDetails) {
    const personName = buildPersonName(personDetails);
    
    return {
        connectedEntity: {
            INN: personDetails?.INN || null,
            NameShort: personName,
            NameFull: personDetails?.FullName || personName,
            type: 'physical',
            sourceTable: `prevwork_person_from_${personDetails?.sourceTable || 'unknown'}`,
            source: 'local',
            baseName: personDetails?.baseName,
            PersonUNID: record.PersonUNID,
            ...personDetails
        },
        connectionType: 'org_inn_to_prev_worker_match',
        connectionStatus: 'former_employee',
        connectionDetails: `Бывший сотрудник найден через CF_PrevWork_test по ИНН организации ${record.relatedINN}`
    };
}

function processDirectMatches(records, entitiesByKey, connectionsMap) {
    records.forEach(record => {
        const { relatedINN: targetPersonINN } = record;
        const connectionInfo = createDirectMatchConnection(record);
        
        findAndAddConnections(entitiesByKey, connectionsMap, targetPersonINN, connectionInfo);
    });
}

function createDirectMatchConnection(record) {
    const personName = record.contactNameShort || record.fzFIO || record.relatedINN;
    
    return {
        connectedEntity: {
            INN: record.relatedINN,
            NameShort: personName,
            NameFull: record.contactNameFull || record.fzFIO || personName,
            type: 'physical',
            sourceTable: record.sourceTable,
            source: 'local',
            baseName: record.baseName,
            PersonUNID: record.PersonUNID,
            FirstName: record.PersonFirstName,
            LastName: record.PersonLastName,
            MiddleName: record.PersonMiddleName,
            SNILS: record.PersonSNILS
        },
        connectionType: 'inn_match',
        connectionStatus: 'person_match',
        connectionDetails: `Найден человек в CF_Persons_test по ИНН ${record.relatedINN}`
    };
}

function processOtherRecords(records, entitiesByKey, connectionsMap) {
    records.forEach(record => {
        const connectionInfo = createOtherRecordConnection(record);
        
        // Для разных типов записей используем разные ИНН для поиска
        if (shouldUseFoundINN(record)) {
            findAndAddConnections(entitiesByKey, connectionsMap, record.contactINN, connectionInfo);
        } else if (shouldUseRelatedINN(record)) {
            findAndAddConnections(entitiesByKey, connectionsMap, record.relatedINN, connectionInfo);
        }
    });
}

function createOtherRecordConnection(record) {
    const { connectionType, connectionStatus } = determineConnectionType(record);
    const connectedName = record.contactNameShort || record.contactNameFull || record.contactINN || 'N/A';
    
    const connection = {
        connectedEntity: {
            INN: record.contactINN,
            NameShort: connectedName,
            NameFull: record.contactNameFull,
            type: determineEntityType(record),
            sourceTable: record.sourceTable,
            source: 'local',
            baseName: record.baseName,
            PersonUNID: record.PersonUNID
        },
        connectionType: connectionType,
        connectionStatus: connectionStatus,
        connectionDetails: `Совпадение по ИНН: ${record.contactINN}, таблица: ${record.sourceTable}`
    };
    
    // Добавляем информацию о сотруднике если есть
    if (record.sourceTable === 'employee') {
        connection.employeeInfo = {
            fzFIO: record.fzFIO,
            phFunction: record.phFunction,
            phEventType: record.phEventType,
            phDate: record.phDate ? new Date(record.phDate).toLocaleDateString() : null
        };
    }
    
    return connection;
}

// --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ДЛЯ ОПРЕДЕЛЕНИЯ ТИПОВ ---

function shouldUseFoundINN(record) {
    const excludedTypes = [
        'employee_by_person_inn', 
        'person_by_inn_via_prevwork', 
        'person_direct_inn_match', 
        'prevwork_by_org_inn'
    ];
    return !excludedTypes.includes(record.sourceTable);
}

function shouldUseRelatedINN(record) {
    return ['employee_by_person_inn', 'person_by_inn_via_prevwork'].includes(record.sourceTable);
}

function determineConnectionType(record) {
    const types = {
        'contragent': { type: 'inn_match', status: 'organization_match' },
        'prevwork': { type: 'inn_match', status: 'former_employee' },
        'employee': { type: 'inn_match', status: getEmployeeStatus(record.phEventType) },
        'contperson': { type: 'inn_match', status: 'contact_person' },
        'employee_by_person_inn': { type: 'person_inn_to_org_match', status: getEmployeeStatus(record.phEventType) },
        'person_by_inn_via_prevwork': { type: 'person_inn_to_org_match', status: 'former_workplace_of' }
    };
    
    return types[record.sourceTable] || { type: 'inn_match', status: 'unknown_status' };
}

function getEmployeeStatus(eventType) {
    return eventType?.toLowerCase().includes('увол') ? 'former_employee' : 'current_employee';
}

// --- ОБЩИЕ ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---

function findAndAddConnections(entitiesByKey, connectionsMap, searchINN, connectionInfo) {
    entitiesByKey.forEach((targetEntity, targetEntityKey) => {
        if (targetEntity.type === 'prevwork') return;
        if (targetEntity.INN !== searchINN) return;
        
        if (!connectionsMap.has(targetEntityKey)) {
            connectionsMap.set(targetEntityKey, {});
        }
        
        if (!connectionsMap.get(targetEntityKey)[searchINN]) {
            connectionsMap.get(targetEntityKey)[searchINN] = [];
        }
        
        connectionsMap.get(targetEntityKey)[searchINN].push(connectionInfo);
    });
}

function buildPersonName(personDetails) {
    if (!personDetails) return 'ФИО не найдено';
    
    if (personDetails.NameForDisplay) return personDetails.NameForDisplay;
    if (personDetails.FullName) return personDetails.FullName;
    
    const { FirstName, LastName, MiddleName } = personDetails;
    if (FirstName || LastName || MiddleName) {
        return `${LastName || ''} ${FirstName || ''} ${MiddleName || ''}`.trim();
    }
    
    return 'ФИО не найдено';
}

// --- ФУНКЦИЯ ПОИСКА ДЕТАЛЕЙ ПЕРСОН (оставляем без изменений, но можно тоже упростить) ---
async function findPersonDetailsByUNID(personUNIDs) {
    console.log("Поиск деталей для PersonUNIDs:", personUNIDs);

    const personDetailsMap = new Map();
    if (!personUNIDs?.length) return personDetailsMap;

    try {
        const personResult = await executePersonDetailsQuery(personUNIDs);
        console.log("Найдено деталей персон:", personResult.recordset.length);

        personResult.recordset.forEach(row => {
            processPersonDetailRow(row, personDetailsMap);
        });

    } catch (err) {
        console.error('Ошибка при поиске деталей физических лиц:', err);
    }

    return personDetailsMap;
}

async function executePersonDetailsQuery(personUNIDs) {
    const unidParams = personUNIDs.map((unid, index) => `@unid${index}`);
    
    const query = `
        SELECT
            p.UNID as PersonUNID,
            p.FirstName, p.LastName, p.MiddleName, p.SNILS, p.BirthDate,
            'persons' as sourceTable, NULL as baseName
        FROM CF_Persons_test p WHERE p.UNID IN (${unidParams.join(', ')})
        
        UNION ALL
        
        SELECT
            ce.fzUID as PersonUNID,
            NULL as FirstName, NULL as LastName, NULL as MiddleName,
            ce.fzFIO as FullName, ce.fzINN as SNILS, NULL as BirthDate,
            'employees' as sourceTable, ce.BaseName as baseName
        FROM CI_Employees_test ce WHERE ce.fzUID IN (${unidParams.join(', ')})
        
        UNION ALL
        
        SELECT
            cip.cpUID as PersonUNID,
            cip.cpName1 as FirstName, cip.cpName2 as LastName, cip.cpName3 as MiddleName,
            cip.cpNameFull as FullName, NULL as SNILS, NULL as BirthDate,
            'contpersons' as sourceTable, cip.BaseName as baseName
        FROM CI_ContPersons_test cip WHERE cip.cpUID IN (${unidParams.join(', ')})
        
        UNION ALL
        
        SELECT
            cc.PersonUNID,
            NULL as FirstName, NULL as LastName, NULL as MiddleName,
            NULL as FullName, NULL as SNILS, NULL as BirthDate,
            'contacts' as sourceTable, NULL as baseName
        FROM CF_Contacts_test cc WHERE cc.PersonUNID IN (${unidParams.join(', ')})
    `;

    const request = new sql.Request();
    personUNIDs.forEach((unid, index) => {
        request.input(`unid${index}`, sql.VarChar, unid);
    });

    return await request.query(query);
}

function processPersonDetailRow(row, personDetailsMap) {
    if (!row.PersonUNID) return;
    
    if (!personDetailsMap.has(row.PersonUNID)) {
        const details = {
            PersonUNID: row.PersonUNID,
            FirstName: row.FirstName,
            LastName: row.LastName,
            MiddleName: row.MiddleName,
            FullName: row.FullName,
            SNILS: row.SNILS,
            BirthDate: row.BirthDate,
            sourceTable: row.sourceTable,
            baseName: row.baseName,
            NameForDisplay: buildDisplayName(row)
        };
        personDetailsMap.set(row.PersonUNID, details);
    }
}

function buildDisplayName(row) {
    if (row.FirstName && row.LastName) {
        return `${row.LastName} ${row.FirstName} ${row.MiddleName || ''}`.trim();
    }
    return row.FullName || 'ФИО не найдено';
}

export { findConnectionsByINN };