import sql from 'mssql'
import { determineEntityType, getEntityKey } from '../utils/helper.js';
import { buildINNQuery } from '../queries/inn.queries.js';

// --- КОНСТАНТЫ И КОНФИГУРАЦИЯ ---
const SEARCH_TYPES = {
    PREVWORK_BY_ORG: 'prevwork_by_org_inn',
    PERSON_DIRECT: 'person_direct_inn_match',
    CONTRAGENT: 'contragent',
    EMPLOYEE: 'employee',
    CONT_PERSON: 'contperson',
    EMPLOYEE_BY_PERSON: 'employee_by_person_inn',
    PERSON_VIA_PREVWORK: 'person_by_inn_via_prevwork'
};

const CONNECTION_STATUS = {
    ORGANIZATION_MATCH: 'organization_match',
    FORMER_EMPLOYEE: 'former_employee',
    CURRENT_EMPLOYEE: 'current_employee',
    CONTACT_PERSON: 'contact_person',
    FORMER_EMPLOYEE_OF: 'former_employee_of',
    CURRENT_EMPLOYEE_OF: 'current_employee_of',
    FORMER_WORKPLACE: 'former_workplace_of',
    PERSON_MATCH: 'person_match'
};

// --- ОСНОВНАЯ ФУНКЦИЯ ---
async function findConnectionsByINN(targetEntities) {
    console.log("Запуск findConnectionsByINN");

    const targetINNs = new Set();
    const entitiesByKey = new Map();

    targetEntities.forEach(entity => {
        if (entity.type === 'prevwork') {
            // console.log(`Пропускаем prevwork сущность с ИНН ${entity.INN}`);
            return;
        }

        const entityKey = getEntityKey(entity);
        if (entityKey) {
            // Сохраняем ВСЕ ИНН сущности
            const allINNs = getAllINNs(entity);
            entitiesByKey.set(entityKey, { 
                ...entity, 
                allINNs: allINNs // Сохраняем все ИНН для использования в сопоставлении
            });
            
            // Добавляем все ИНН в поиск
            allINNs.forEach(inn => {
                if (inn && inn.trim() !== '') {
                    targetINNs.add(inn);
                }
            });
        }
    });

    const innArray = Array.from(targetINNs).filter(inn => inn);
    console.log("Целевые ИНН для поиска связей:", innArray);
    console.log("entitiesByKey размер:", entitiesByKey.size);

    if (innArray.length === 0) {
        console.log("Нет ИНН для поиска связей");
        return createEmptyConnectionsMap(entitiesByKey);
    }

    const connectionsMap = createEmptyConnectionsMap(entitiesByKey);

    try {
        const innResult = await executeINNQuery(innArray);
        console.log("Найдено результатов по ИНН:", innResult.recordset.length);

        await processSearchResults(innResult.recordset, entitiesByKey, connectionsMap);

    } catch (err) {
        console.error('Ошибка при поиске связей по ИНН:', err);
        throw err;
    }

    console.log(`Итоговый размер connectionsMap: ${connectionsMap.size}`);
    return connectionsMap;
}

// Новая функция для получения всех ИНН сущности
function getAllINNs(entity) {
    const inns = new Set();
    
    // Основной ИНН (уже нормализованный)
    if (entity.INN && entity.INN.trim() !== '') {
        inns.add(entity.INN);
    }
    
    // ИНН организации для сотрудников (phOrgINN)
    if (entity.phOrgINN && entity.phOrgINN.trim() !== '') {
        inns.add(entity.phOrgINN);
    }
    
    // Личный ИНН сотрудника (fzINN) 
    if (entity.fzINN && entity.fzINN.trim() !== '' && entity.fzINN !== entity.INN) {
        inns.add(entity.fzINN);
    }
    
    // ИНН организации для контактных лиц (conINN)
    if (entity.conINN && entity.conINN.trim() !== '' && entity.conINN !== entity.INN) {
        inns.add(entity.conINN);
    }
    
    console.log(`Сущность ${getEntityKey(entity)} имеет ИНН:`, Array.from(inns));
    return Array.from(inns);
}

// --- ПОДГОТОВКА ДАННЫХ ---
function prepareSearchData(targetEntities) {
    const targetINNs = new Set();
    const entitiesByKey = new Map();

    targetEntities.forEach(entity => {
        if (entity.type === 'prevwork') {
            // console.log(`Пропускаем prevwork сущность с ИНН ${entity.INN}`);
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

// --- ОБРАБОТКА РЕЗУЛЬТАТОВ ---
async function processSearchResults(records, entitiesByKey, connectionsMap) {
    // console.log('=== DEBUG processSearchResults ===');
    // console.log('Всего записей из SQL:', records.length);
    // console.log('Первые 5 записей:', records.slice(0, 5).map(r => ({
    //     sourceTable: r.sourceTable,
    //     contactINN: r.contactINN,
    //     relatedINN: r.relatedINN,
    //     contactNameShort: r.contactNameShort
    // })));

    const recordsByType = groupRecordsByType(records);
    // console.log('Группировка записей:', {
    //     prevwork: recordsByType.prevwork.length,
    //     direct: recordsByType.direct.length, 
    //     other: recordsByType.other.length
    // });
    
    // Обрабатываем записи о предыдущих местах работы
    if (recordsByType.prevwork.length > 0) {
        console.log('Обрабатываем prevwork записи...');
        await processPrevWorkRecords(recordsByType.prevwork, entitiesByKey, connectionsMap);
    }
    
    // Обрабатываем прямые совпадения по ИНН
    if (recordsByType.direct.length > 0) {
        console.log('Обрабатываем direct записи...');
        processDirectMatches(recordsByType.direct, entitiesByKey, connectionsMap);
    }
    
    // Обрабатываем остальные записи
    if (recordsByType.other.length > 0) {
        console.log('Обрабатываем other записи...');
        processOtherRecords(recordsByType.other, entitiesByKey, connectionsMap);
    }

    // Проверим результат
    // console.log('=== РЕЗУЛЬТАТ connectionsMap ===');
    let totalConnections = 0;
    // connectionsMap.forEach((connections, key) => {
    //     const count = Object.values(connections).flat().length;
    //     totalConnections += count;
    //     if (count > 0) {
    //         console.log(`Сущность ${key}: ${count} связей`);
    //     }
    // });
    console.log(`Всего связей найдено: ${totalConnections}`);
}

function groupRecordsByType(records) {
    const groups = {
        prevwork: [],
        direct: [],
        other: []
    };
    
    records.forEach(record => {
        if (record.sourceTable === SEARCH_TYPES.PREVWORK_BY_ORG) {
            groups.prevwork.push(record);
        } else if (record.sourceTable === SEARCH_TYPES.PERSON_DIRECT) {
            groups.direct.push(record);
        } else {
            groups.other.push(record);
        }
    });
    
    return groups;
}

// --- ОБРАБОТКА PREVWORK ЗАПИСЕЙ ---
async function processPrevWorkRecords(records, entitiesByKey, connectionsMap) {
    const personUNIDs = [...new Set(records.map(r => r.PersonUNID).filter(Boolean))];
    const personDetailsMap = await findPersonDetailsByUNID(personUNIDs);
    
    for (const record of records) {
        await processSinglePrevWorkRecord(record, entitiesByKey, connectionsMap, personDetailsMap);
    }
}

async function processSinglePrevWorkRecord(record, entitiesByKey, connectionsMap, personDetailsMap) {
    const { relatedINN: orgINN, PersonUNID } = record;
    
    if (!PersonUNID) return;
    
    const personDetails = personDetailsMap.get(PersonUNID);
    const connectionInfo = createPrevWorkConnection(record, personDetails);
    
    addConnectionToTargets(entitiesByKey, connectionsMap, orgINN, connectionInfo);
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
        connectionStatus: CONNECTION_STATUS.FORMER_EMPLOYEE,
        connectionDetails: `Бывший сотрудник найден через CF_PrevWork_test по ИНН организации ${record.relatedINN}`
    };
}

// --- ОБРАБОТКА ПРЯМЫХ СОВПАДЕНИЙ ---
function processDirectMatches(records, entitiesByKey, connectionsMap) {
    records.forEach(record => {
        const { relatedINN: personINN } = record;
        const connectionInfo = createDirectMatchConnection(record);
        
        addConnectionToTargets(entitiesByKey, connectionsMap, personINN, connectionInfo);
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
        connectionStatus: CONNECTION_STATUS.PERSON_MATCH,
        connectionDetails: `Найден человек в CF_Persons_test по ИНН ${record.relatedINN}`
    };
}

// --- ОБРАБОТКА ОСТАЛЬНЫХ ЗАПИСЕЙ ---
function processOtherRecords(records, entitiesByKey, connectionsMap) {
    // console.log('=== DEBUG processOtherRecords ===');
    
    records.forEach(record => {
        // console.log(`Обработка записи: ${record.sourceTable}`, {
        //     contactINN: record.contactINN,
        //     relatedINN: record.relatedINN,
        //     useFoundINN: shouldUseFoundINN(record),
        //     useRelatedINN: shouldUseRelatedINN(record)
        // });

        const connectionInfo = createOtherRecordConnection(record);
        
        if (shouldUseFoundINN(record)) {
            // console.log(`  Используем foundINN: ${record.contactINN}`);
            addConnectionToTargets(entitiesByKey, connectionsMap, record.contactINN, connectionInfo);
        } else if (shouldUseRelatedINN(record)) {
            // console.log(`  Используем relatedINN: ${record.relatedINN}`);
            addConnectionToTargets(entitiesByKey, connectionsMap, record.relatedINN, connectionInfo);
        } else {
            // console.log(`  НЕ ИСПОЛЬЗУЕТСЯ: ${record.sourceTable}`);
        }
    });
}

function createOtherRecordConnection(record) {
    const { connectionType, connectionStatus } = determineConnectionInfo(record);
    // console.log('Другая связь: ', connectionType,connectionStatus, record);
    const connectedName = record.contactNameShort || record.contactNameFull || record.contactINN || 'N/A';

    
    // Создаем правильную связанную сущность с ВСЕМИ нужными полями
    const connectedEntity = {
        INN: record.contactINN,
        NameShort: connectedName,
        NameFull: record.contactNameFull,
        sourceTable: record.sourceTable,
        source: 'local',
        baseName: record.baseName,
        PersonUNID: record.PersonUNID
    };
    
    // ДОБАВЛЯЕМ поля для определения типа
    if (record.UrFiz !== undefined) connectedEntity.UrFiz = record.UrFiz;
    if (record.fIP !== undefined) connectedEntity.fIP = record.fIP;
    
    // ТЕПЕРЬ определяем тип
    connectedEntity.type = determineEntityType(connectedEntity);
    
    const connection = {
        connectedEntity: connectedEntity,
        connectionType: connectionType,
        connectionStatus: connectionStatus,
        connectionDetails: buildConnectionDetails(record, connectionStatus)
    };
    
    // Добавляем информацию о сотруднике если есть
    if (record.sourceTable === SEARCH_TYPES.EMPLOYEE || record.sourceTable === SEARCH_TYPES.EMPLOYEE_BY_PERSON) {
        connection.employeeInfo = extractEmployeeInfo(record);
    }
    
    return connection;
}

// --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---
function addConnectionToTargets(entitiesByKey, connectionsMap, searchINN, connectionInfo) {
    // console.log(`=== addConnectionToTargets: searchINN=${searchINN} ===`);
    
    let matches = 0;
    entitiesByKey.forEach((targetEntity, targetEntityKey) => {
        if (targetEntity.type === 'prevwork') {
            return;
        }
        
        // Проверяем ВСЕ ИНН сущности, а не только entity.INN
        const entityINNs = targetEntity.allINNs || [targetEntity.INN];
        const hasMatchingINN = entityINNs.includes(searchINN);
        
        if (!hasMatchingINN) {
            // console.log(`  INN не совпадает: target имеет ${entityINNs}, search=${searchINN}`);
            return;
        }

        // Проверка на самосвязь
        const connectedEntity = connectionInfo.connectedEntity;
        if (getEntityKey(targetEntity) === getEntityKey(connectedEntity)) {
            // console.log(`  Пропускаем самосвязь: ${targetEntityKey}`);
            return;
        }

        // console.log(`  НАЙДЕНО СОВПАДЕНИЕ: ${targetEntityKey} с INN ${searchINN}`);
        matches++;

        if (!connectionsMap.has(targetEntityKey)) {
            connectionsMap.set(targetEntityKey, {});
        }
        
        if (!connectionsMap.get(targetEntityKey)[searchINN]) {
            connectionsMap.get(targetEntityKey)[searchINN] = [];
        }
        
        connectionsMap.get(targetEntityKey)[searchINN].push(connectionInfo);
    });

    // console.log(`  Всего совпадений для INN ${searchINN}: ${matches}`);
}

function shouldUseFoundINN(record) {
    const excludedTypes = [
        SEARCH_TYPES.EMPLOYEE_BY_PERSON, 
        SEARCH_TYPES.PERSON_VIA_PREVWORK, 
        SEARCH_TYPES.PERSON_DIRECT, 
        SEARCH_TYPES.PREVWORK_BY_ORG
    ];
    return !excludedTypes.includes(record.sourceTable);
}

function shouldUseRelatedINN(record) {
    // Для employee_by_person_inn используем relatedINN только если это НЕ сам сотрудник
    if (record.sourceTable === SEARCH_TYPES.EMPLOYEE_BY_PERSON) {
        // Проверяем, что это не сам сотрудник
        return record.contactINN !== record.relatedINN;
    }
    return [SEARCH_TYPES.EMPLOYEE_BY_PERSON, SEARCH_TYPES.PERSON_VIA_PREVWORK].includes(record.sourceTable);
}

function determineConnectionInfo(record) {
    
    const connectionMap = {
        [SEARCH_TYPES.CONTRAGENT]: { 
            connectionType: 'inn_match', 
            connectionStatus: CONNECTION_STATUS.ORGANIZATION_MATCH 
        },
        [SEARCH_TYPES.EMPLOYEE]: { 
            connectionType: 'inn_match', 
            connectionStatus: getEmployeeStatus(record.phEventType) 
        },
        [SEARCH_TYPES.CONT_PERSON]: { 
            connectionType: 'inn_match', 
            connectionStatus: CONNECTION_STATUS.CONTACT_PERSON 
        },
        [SEARCH_TYPES.EMPLOYEE_BY_PERSON]: { 
            connectionType: 'person_inn_to_org_match', 
            connectionStatus: getEmployeeStatus(record.phEventType) 
        },
        [SEARCH_TYPES.PERSON_VIA_PREVWORK]: { 
            connectionType: 'person_inn_to_org_match', 
            connectionStatus: CONNECTION_STATUS.FORMER_WORKPLACE 
        },
        [SEARCH_TYPES.PERSON_DIRECT]: {
            connectionType: 'inn_match',
            connectionStatus: CONNECTION_STATUS.PERSON_MATCH
        },
        [SEARCH_TYPES.PREVWORK_BY_ORG]: {
            connectionType: 'org_inn_to_prev_worker_match', 
            connectionStatus: CONNECTION_STATUS.FORMER_EMPLOYEE
        }
    };
    
    const result = connectionMap[record.sourceTable] || { 
        connectionType: 'inn_match', 
        connectionStatus: CONNECTION_STATUS.ORGANIZATION_MATCH
    };
    
    return result;
}
function getEmployeeStatus(eventType) {
    return eventType?.toLowerCase().includes('увол') 
        ? CONNECTION_STATUS.FORMER_EMPLOYEE 
        : CONNECTION_STATUS.CURRENT_EMPLOYEE;
}

function buildConnectionDetails(record, status) {
    return `Совпадение по ИНН: ${record.contactINN}, таблица: ${record.sourceTable}, статус: ${status}`;
}

function extractEmployeeInfo(record) {
    return {
        fzFIO: record.fzFIO,
        phFunction: record.phFunction,
        phEventType: record.phEventType,
        phDate: record.phDate ? new Date(record.phDate).toLocaleDateString() : null
    };
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

// --- ФУНКЦИЯ ПОИСКА ДЕТАЛЕЙ ПЕРСОН (упрощенная версия) ---
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
        NULL as FullName, -- ДОБАВЛЯЕМ этот столбец
        'persons' as sourceTable, 
        NULL as baseName
    FROM CF_Persons_test p 
    WHERE p.UNID IN (${unidParams.join(', ')})
    
    UNION ALL
    
    SELECT
        ce.fzUID as PersonUNID,
        NULL as FirstName, NULL as LastName, NULL as MiddleName,
        ce.fzINN as SNILS, NULL as BirthDate,
        ce.fzFIO as FullName, -- Уже есть
        'employees' as sourceTable, 
        ce.BaseName as baseName
    FROM CI_Employees_test ce 
    WHERE ce.fzUID IN (${unidParams.join(', ')})
    
    UNION ALL
    
    SELECT
        cip.cpUID as PersonUNID,
        cip.cpName1 as FirstName, cip.cpName2 as LastName, cip.cpName3 as MiddleName,
        NULL as SNILS, NULL as BirthDate,
        cip.cpNameFull as FullName, -- Уже есть
        'contpersons' as sourceTable, 
        cip.BaseName as baseName
    FROM CI_ContPersons_test cip 
    WHERE cip.cpUID IN (${unidParams.join(', ')})
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