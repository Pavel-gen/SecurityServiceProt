import sql from 'mssql'
import { determineEntityType } from '../utils/helper.js';
import { getEntityKey } from '../utils/helper.js';
import { buildINNQuery } from '../queries/inn.queries.js';



// --- ОСНОВНАЯ ФУНКЦИЯ: Поиск связей по ИНН ---
// --- ОСНОВНАЯ ФУНКЦИЯ: Поиск связей по ИНН ---
async function findConnectionsByINN(targetEntities) {
    console.log("Запуск findConnectionsByINN");

    const targetINNs = new Set();
    const entitiesByKey = new Map();

    targetEntities.forEach(entity => {
        // Исключаем prevwork из рассмотрения как целевых сущностей для ИНН-поиска
        // и из построения connectionsMap по ИНН
        if (entity.type === 'prevwork') {
            console.log(`Сущность prevwork с ИНН ${entity.INN} исключена из поиска по ИНН.`, entity);
            return; // Пропускаем
        }

        const entityKey = getEntityKey(entity);
        if (entityKey && entity.INN && entity.INN.trim() !== '') {
            targetINNs.add(entity.INN);
            entitiesByKey.set(entityKey, entity);
            console.log(`Добавлена сущность с ключом ${entityKey} и ИНН ${entity.INN} в entitiesByKey`);
        } else {
            console.log(`Сущность не имеет ключа или ИНН, пропускаем:`, entity);
        }
    });

    const innArray = Array.from(targetINNs).filter(inn => inn);
    console.log("Целевые ИНН для поиска связей:", innArray);
    console.log("entitiesByKey размер:", entitiesByKey.size);

    if (innArray.length === 0) {
        console.log("Нет ИНН для поиска связей (юр/физ лица).");
        const connectionsMap = new Map();
        entitiesByKey.forEach((entity, entityKey) => {
            if (!connectionsMap.has(entityKey)) {
                connectionsMap.set(entityKey, {});
            }
        });
        // processPrevWork больше не вызывается здесь
        return connectionsMap;
    }

    const connectionsMap = new Map();
    entitiesByKey.forEach((entity, entityKey) => {
        if (!connectionsMap.has(entityKey)) {
            connectionsMap.set(entityKey, {});
        }
    });

    // processPrevWork больше не вызывается здесь

    const fullINNQuery = buildINNQuery(innArray);
    const innRequest = new sql.Request();
    innArray.forEach((inn, index) => {
        innRequest.input(`inn${index}`, sql.VarChar, inn);
    });

    // Создаем общую карту для деталей персон
    const initialPersonDetailsMap = new Map();

    try {
        const innResult = await innRequest.query(fullINNQuery);
        console.log("Количество результатов поиска по ИНН (юр/физ лица):", innResult.recordset.length);

        await mapFoundEntitiesToTargets(innResult.recordset, entitiesByKey, connectionsMap, innArray, initialPersonDetailsMap);

    } catch (err) {
        console.error('Ошибка при поиске связей по ИНН (юр/физ лица):', err);
        throw err;
    }

    console.log(`Итоговый размер connectionsMap (ИНН): ${connectionsMap.size}`);
    return connectionsMap;
}


// Добавим personDetailsMap как параметр, чтобы не запрашивать одни и те же данные несколько раз
async function mapFoundEntitiesToTargets(foundRecordset, entitiesByKey, connectionsMap, targetINNs, personDetailsMap = new Map()) {
    console.log("Начало сопоставления найденных сущностей с целевыми");
    console.log('connectionMap: ', connectionsMap);

    // Соберем PersonUNID, которые нужно будет обогатить, из результатов prevwork_by_org_inn
    const personUNIDsToEnrich = new Set();
    foundRecordset.forEach(row => {
        if (row.sourceTable === 'prevwork_by_org_inn') {
            if (row.PersonUNID) {
                personUNIDsToEnrich.add(row.PersonUNID);
            }
        }
    });

    // Запросим информацию о людях, если есть что запрашивать
    let enrichedPersonDetailsMap = personDetailsMap;
    if (personUNIDsToEnrich.size > 0) {
        const personUNIDsToFetch = [...personUNIDsToEnrich].filter(unid => !enrichedPersonDetailsMap.has(unid));
        if (personUNIDsToFetch.length > 0) {
            const newlyFetchedDetails = await findPersonDetailsByUNID(personUNIDsToFetch);
            for (const [key, value] of newlyFetchedDetails) {
                enrichedPersonDetailsMap.set(key, value);
            }
        }
    }


    foundRecordset.forEach(row => {
        // --- Обработка prevwork_by_org_inn отдельно ---
        if (row.sourceTable === 'prevwork_by_org_inn') {
            const relatedOrgINN = row.relatedINN; // ИНН организации, по которому искали (target INN)
            const personUNIDFromPrevWork = row.PersonUNID; // PersonUNID человека из prevwork

            if (!personUNIDFromPrevWork) {
                 console.log(`Предупреждение: prevwork_by_org_inn запись не содержит PersonUNID. Пропускаем.`, row);
                 return;
            }

            // Найдем информацию о человеке
            const personDetails = enrichedPersonDetailsMap.get(personUNIDFromPrevWork);
            let personName = 'ФИО не найдено';
            let personType = 'physical';
            let personSourceTable = 'prevwork_person_details_unknown';
            let personBaseName = null;
            let personINN = null;

            if (personDetails) {
                personName = personDetails.NameForDisplay || personDetails.FullName || `${personDetails.FirstName || ''} ${personDetails.LastName || ''} ${personDetails.MiddleName || ''}`.trim() || 'ФИО не найдено';
                // personType уже 'physical'
                personSourceTable = `prevwork_person_from_${personDetails.sourceTable}`;
                personBaseName = personDetails.baseName;
                personINN = personDetails.INN;
            }

            // Найдем целевую сущность по relatedOrgINN
            entitiesByKey.forEach((targetEntity, targetEntityKey) => {
                if (targetEntity.type === 'prevwork') {
                     return; // Пропускаем prevwork в этой части
                }

                if (targetEntity.INN === relatedOrgINN) {
                    const targetINN = relatedOrgINN;

                    if (!connectionsMap.has(targetEntityKey)) {
                         connectionsMap.set(targetEntityKey, {});
                    }
                    if (!connectionsMap.get(targetEntityKey)[targetINN]) {
                         connectionsMap.get(targetEntityKey)[targetINN] = [];
                    }

                    connectionsMap.get(targetEntityKey)[targetINN].push({
                        connectedEntity: {
                            INN: personINN || null,
                            NameShort: personName,
                            NameFull: personDetails?.FullName || personName,
                            type: personType,
                            sourceTable: personSourceTable,
                            source: 'local',
                            baseName: personBaseName,
                            PersonUNID: personUNIDFromPrevWork
                        },
                        connectionType: 'org_inn_to_prev_worker_match',
                        connectionStatus: 'former_employee',
                        connectionDetails: `Бывший сотрудник, найден через CF_PrevWork_test (PersonUNID: ${personUNIDFromPrevWork}) по ИНН организации ${targetINN}. Подробности из: ${personDetails?.sourceTable || 'unknown'}.`
                    });
                }
            });

            return; // Завершаем обработку этой строки
        }

        // --- Обработка person_direct_inn_match отдельно ---
        // Этот тип означает, что по ИНН найдена сама персона в CF_Persons_test
        if (row.sourceTable === 'person_direct_inn_match') {
            const targetPersonINN = row.relatedINN; // ИНН физлица, по которому искали (target INN)

            // Сформируем имя из результата SQL (оно уже там собрано)
            let personName = row.contactNameShort || row.fzFIO || targetPersonINN; // fallback на ИНН
            let personNameFull = row.contactNameFull || row.fzFIO || targetPersonINN;
            let personLastName = row.PersonLastName || null;
            let personFirstName = row.PersonFirstName || null;
            let personMiddleName = row.PersonMiddleName || null;
            let personSNILS = row.PersonSNILS || null;

            // Найдем целевую сущность по targetPersonINN
            entitiesByKey.forEach((targetEntity, targetEntityKey) => {
                if (targetEntity.type === 'prevwork') {
                     return; // Пропускаем prevwork в этой части
                }

                if (targetEntity.INN === targetPersonINN) {
                    const targetINN = targetPersonINN;

                    if (!connectionsMap.has(targetEntityKey)) {
                         connectionsMap.set(targetEntityKey, {});
                    }
                    if (!connectionsMap.get(targetEntityKey)[targetINN]) {
                         connectionsMap.get(targetEntityKey)[targetINN] = [];
                    }

                    // connectedEntity - это физлицо (человек, чей ИНН === targetINN)
                    connectionsMap.get(targetEntityKey)[targetINN].push({
                        connectedEntity: {
                            INN: targetPersonINN, // ИНН физлица (target)
                            NameShort: personName, // <<< ФИО физлица из CF_Persons
                            NameFull: personNameFull, // <<< Полное имя
                            type: 'physical', // Тип - физлицо
                            sourceTable: row.sourceTable, // 'person_direct_inn_match'
                            source: 'local',
                            baseName: targetEntity.baseName || row.baseName, // BaseName из целевой сущности или из результата
                            PersonUNID: row.PersonUNID, // PersonUNID из CF_Persons
                            // Дополнительные поля из CF_Persons, если нужно
                            FirstName: personFirstName,
                            LastName: personLastName,
                            MiddleName: personMiddleName,
                            SNILS: personSNILS
                        },
                        connectionType: 'inn_match', // Тип связи - совпадение по ИНН
                        connectionStatus: 'person_match', // Статус - найден сам человек
                        connectionDetails: `Найден человек в CF_Persons_test по ИНН ${targetINN}.`
                    });
                }
            });

            return; // Завершаем обработку этой строки
        }

        // --- Обработка остальных типов источников ---
        let connectedType = 'unknown';
        let connectedName = row.contactNameShort || row.contactNameFull || row.contactINN || 'N/A';
        let connectedEntityKey = row.entityKey;
        let connectionStatus = 'unknown_status';
        let baseName = row.baseName || null;

        let fzFIO = row.fzFIO || null;
        let phFunction = row.phFunction || null;
        let phEventType = row.phEventType || null;
        let phDate = row.phDate ? new Date(row.phDate).toLocaleDateString() : null;

        // --- Извлечение ФИО из CF_Persons для person_by_inn_via_prevwork ---
        let personLastName = row.PersonLastName || null;
        let personFirstName = row.PersonFirstName || null;
        let personMiddleName = row.PersonMiddleName || null;
        let personFIOFromPersons = null;
        if (personLastName || personFirstName || personMiddleName) {
            personFIOFromPersons = `${personLastName || ''} ${personFirstName || ''} ${personMiddleName || ''}`.trim();
        }

        if (row.sourceTable === 'contragent') {
            connectedType = determineEntityType(row.UrFiz, row.fIP);
            connectionStatus = 'organization_match';
        } else if (row.sourceTable === 'prevwork') { // Это старый тип, для prevwork как *цели*
            connectedType = 'physical';
            connectionStatus = 'former_employee';
        } else if (row.sourceTable === 'employee') {
            connectedType = 'physical';
            connectionStatus = (phEventType && phEventType.toLowerCase().includes('увол')) ? 'former_employee' : 'current_employee';
        } else if (row.sourceTable === 'contperson') {
            connectedType = 'physical';
            connectionStatus = 'contact_person';
        } else if (row.sourceTable === 'employee_by_person_inn') {
            connectedType = 'legal';
            connectionStatus = (phEventType && phEventType.toLowerCase().includes('увол')) ? 'former_employee_of' : 'current_employee_of';
        } else if (row.sourceTable === 'person_by_inn_via_prevwork') {
            connectedType = 'legal';
            connectionStatus = 'former_workplace_of';
        } else if (row.sourceTable === 'person_direct_inn_match') { /* Обработано выше */ }
        else if (row.sourceTable === 'prevwork_by_org_inn') { /* Обработано выше */ }

        const foundINN = row.contactINN;
        const relatedPersonINN = row.relatedINN; // Это ИНН физлица, по которому искали


        entitiesByKey.forEach((targetEntity, targetEntityKey) => {
            if (targetEntity.type === 'prevwork') {
                 return;
            }

            // --- Сопоставление для поиска связей ОТ юр/ип к другим сущностям (контрагенты, сотрудники, конт.лица) ---
            if (targetEntity.INN === foundINN && row.sourceTable !== 'employee_by_person_inn' && row.sourceTable !== 'person_by_inn_via_prevwork' && row.sourceTable !== 'person_direct_inn_match' && row.sourceTable !== 'prevwork_by_org_inn') {
                if (connectedEntityKey !== targetEntityKey) {
                    if (!connectionsMap.get(targetEntityKey)[foundINN]) {
                        connectionsMap.get(targetEntityKey)[foundINN] = [];
                    }

                    let employeeInfo = null;
                    if (row.sourceTable === 'employee') {
                        employeeInfo = {
                            fzFIO: fzFIO,
                            phFunction: phFunction,
                            phEventType: phEventType,
                            phDate: phDate
                        };
                    }

                    connectionsMap.get(targetEntityKey)[foundINN].push({
                        connectedEntity: {
                            INN: row.contactINN,
                            NameShort: connectedName,
                            NameFull: row.contactNameFull,
                            type: connectedType,
                            sourceTable: row.sourceTable,
                            source: 'local',
                            baseName: baseName,
                            PersonUNID: row.PersonUNID
                        },
                        connectionType: 'inn_match',
                        connectionStatus: connectionStatus,
                        connectionDetails: `Совпадение по ИНН юрлица: ${foundINN}, найдено в таблице ${row.sourceTable}, статус: ${connectionStatus}`,
                        employeeInfo: employeeInfo
                    });
                }
            }

            // --- Сопоставление для поиска связей ОТ ИНН физлица к организациям ---
            if (targetEntity.INN === relatedPersonINN && (row.sourceTable === 'employee_by_person_inn' || row.sourceTable === 'person_by_inn_via_prevwork')) {
                const targetINN = relatedPersonINN;

                let targetPersonName = targetINN;
                let targetPersonNameFull = targetINN;
                let targetPersonBaseName = targetEntity.baseName || null;
                let targetPersonUNID = targetEntity.PersonUNID || null;

                if (targetEntity.FirstName || targetEntity.LastName || targetEntity.MiddleName) {
                    targetPersonName = `${targetEntity.LastName || ''} ${targetEntity.FirstName || ''} ${targetEntity.MiddleName || ''}`.trim();
                    targetPersonNameFull = targetPersonName;
                } else if (targetEntity.NameShort && targetEntity.NameShort !== targetINN) {
                    targetPersonName = targetEntity.NameShort;
                    targetPersonNameFull = targetEntity.NameFull || targetPersonName;
                } else if (row.sourceTable === 'person_by_inn_via_prevwork' && personFIOFromPersons) {
                    targetPersonName = personFIOFromPersons;
                    targetPersonNameFull = personFIOFromPersons;
                } else if (row.sourceTable === 'employee_by_person_inn' && fzFIO) {
                    targetPersonName = fzFIO;
                    targetPersonNameFull = fzFIO;
                }

                if (!connectionsMap.has(targetEntityKey)) {
                     connectionsMap.set(targetEntityKey, {});
                }
                if (!connectionsMap.get(targetEntityKey)[targetINN]) {
                     connectionsMap.get(targetEntityKey)[targetINN] = [];
                }

                connectionsMap.get(targetEntityKey)[targetINN].push({
                    connectedEntity: {
                        INN: targetINN,
                        NameShort: targetPersonName,
                        NameFull: targetPersonNameFull,
                        type: 'physical',
                        sourceTable: row.sourceTable,
                        source: 'local',
                        baseName: targetPersonBaseName,
                        PersonUNID: targetPersonUNID
                    },
                    connectionType: 'person_inn_to_org_match',
                    connectionStatus: connectionStatus,
                    connectionDetails: `Найдена организация (${connectedName}) по ИНН физлица ${targetINN} в таблице ${row.sourceTable}, статус: ${connectionStatus}.`,
                    employeeInfo: row.sourceTable === 'employee_by_person_inn' ? {
                        fzFIO: fzFIO,
                        phFunction: phFunction,
                        phEventType: phEventType,
                        phDate: phDate
                    } : undefined
                });
            }
        });
    });
}

async function findPersonDetailsByUNID(personUNIDs) {
    console.log("Поиск детальной информации о физических лицах для PersonUNIDs:", personUNIDs);

    const personDetailsMap = new Map();

    if (!personUNIDs || personUNIDs.length === 0) {
        console.log("Нет PersonUNID для поиска детальной информации.");
        return personDetailsMap;
    }

    // Подготовим параметры для WHERE
    const unidParams = personUNIDs.map((unid, index) => `@unid${index}`);

    // Запрос 1: Поиск в CF_Persons_test
    const personsQuery = `
        SELECT
            p.UNID as PersonUNID, -- Используем UNID как PersonUNID
            p.FirstName,
            p.LastName,
            p.MiddleName,
            p.SNILS,
            p.BirthDate,
            'persons' as sourceTable,
            NULL as baseName -- BaseName возможно нет в CF_Persons_test
        FROM CF_Persons_test p
        WHERE p.UNID IN (${unidParams.join(', ')})
    `;

    // Запрос 2: Поиск в CI_Employees_test по fzUID (если fzUID совпадает с PersonUNID)
    const employeesQuery = `
        SELECT
            ce.fzUID as PersonUNID,
            NULL as FirstName, -- В CI_Employees FIO в одном поле
            NULL as LastName,
            NULL as MiddleName,
            ce.fzFIO as FullName, -- Используем fzFIO как FullName
            ce.fzINN as SNILS, -- fzINN иногда может быть СНИЛС в этой таблице, уточнить по структуре
            'employees' as sourceTable,
            ce.BaseName as baseName
        FROM CI_Employees_test ce
        WHERE ce.fzUID IN (${unidParams.join(', ')})
    `;

    // Запрос 3: Поиск в CI_ContPersons_test по cpUID (если cpUID совпадает с PersonUNID)
    const contPersonsQuery = `
        SELECT
            cip.cpUID as PersonUNID,
            cip.cpName1 as FirstName,
            cip.cpName2 as LastName,
            cip.cpName3 as MiddleName,
            cip.cpNameFull as FullName,
            NULL as SNILS, -- СНИЛС вряд ли есть в этой таблице
            'contpersons' as sourceTable,
            cip.BaseName as baseName
        FROM CI_ContPersons_test cip
        WHERE cip.cpUID IN (${unidParams.join(', ')})
    `;

    // Запрос 4: Поиск в CF_Contacts_test по PersonUNID
    const contactsQuery = `
        SELECT
            cc.PersonUNID,
            NULL as FirstName,
            NULL as LastName,
            NULL as MiddleName,
            NULL as FullName,
            NULL as SNILS,
            'contacts' as sourceTable,
            NULL as baseName -- BaseName возможно нет в CF_Contacts_test
        FROM CF_Contacts_test cc
        WHERE cc.PersonUNID IN (${unidParams.join(', ')})
    `;

    const fullPersonQuery = `${personsQuery} UNION ALL ${employeesQuery} UNION ALL ${contPersonsQuery} UNION ALL ${contactsQuery}`;

    // console.log("Выполняемый SQL для поиска деталей физ. лиц:", fullPersonQuery);

    const personRequest = new sql.Request();
    personUNIDs.forEach((unid, index) => {
        personRequest.input(`unid${index}`, sql.VarChar, unid);
    });

    try {
        const personResult = await personRequest.query(fullPersonQuery);
        console.log("Количество результатов поиска деталей физ. лиц:", personResult.recordset.length);

        personResult.recordset.forEach(row => {
            const personUNID = row.PersonUNID;
            if (!personUNID) {
                console.log(`Предупреждение: Получена запись без PersonUNID. Пропускаем.`, row);
                return;
            }

            // Если для этого PersonUNID уже есть запись, возможно, из другой таблицы,
            // мы можем попытаться объединить данные, но для простоты просто перезапишем,
            // отдавая приоритет, например, CF_Persons_test, если она есть.
            // Проверим, есть ли уже запись.
            if (!personDetailsMap.has(personUNID)) {
                // Формируем объект с деталями, приоритет у конкретных полей ФИО
                const details = {
                    PersonUNID: personUNID,
                    FirstName: row.FirstName || null,
                    LastName: row.LastName || null,
                    MiddleName: row.MiddleName || null,
                    FullName: row.FullName || null, // Может быть fzFIO или cpNameFull
                    SNILS: row.SNILS || null,
                    BirthDate: row.BirthDate || null,
                    sourceTable: row.sourceTable || 'unknown',
                    baseName: row.baseName || null,
                    // Если не нашли ФИО в отдельных полях, пытаемся получить из FullName
                    NameForDisplay: row.FirstName && row.LastName ? `${row.LastName} ${row.FirstName} ${row.MiddleName || ''}`.trim() : (row.FullName || 'ФИО не найдено')
                };
                personDetailsMap.set(personUNID, details);
                console.log(`Добавлена детальная информация для PersonUNID ${personUNID} из таблицы ${row.sourceTable}.`);
            } else {
                 console.log(`Информация для PersonUNID ${personUNID} уже существует. Текущая: ${personDetailsMap.get(personUNID).sourceTable}, Новая: ${row.sourceTable}.`);
                 // Здесь можно добавить логику объединения, если нужно, но сейчас оставим как есть.
            }
        });

    } catch (err) {
        console.error('Ошибка при поиске деталей физических лиц:', err);
        // Важно: не выбрасываем ошибку, а возвращаем частичный/пустой результат,
        // чтобы основной процесс поиска связей не прерывался.
        // throw err; // Не выбрасываем
    }

    console.log(`Итоговый размер personDetailsMap: ${personDetailsMap.size}`);
    return personDetailsMap;
}

export {
    findConnectionsByINN, // Добавляем новую функцию
};