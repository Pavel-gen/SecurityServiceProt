import sql from 'mssql'
import { determineEntityType } from '../utils/helper.js';
import { getEntityKey } from '../utils/helper.js';



// --- ОСНОВНАЯ ФУНКЦИЯ: Поиск связей по ИНН ---
async function findConnectionsByINN(targetEntities) {
    console.log("Запуск findConnectionsByINN");
    // console.log("Входные targetEntities:", targetEntities);

    // --- ШАГ 1: Подготовка ---
    // Собираем уникальные ИНН из целевых сущностей
    const targetINNs = new Set();
    // Используем getEntityKey для сопоставления
    const entitiesByKey = new Map();

    targetEntities.forEach(entity => {
        const entityKey = getEntityKey(entity);
        if (entityKey && entity.INN && entity.INN.trim() !== '') {
            targetINNs.add(entity.INN);
            entitiesByKey.set(entityKey, entity);
            console.log(`Добавлена сущность с ключом ${entityKey} и ИНН ${entity.INN} в entitiesByKey`);
        } else {
            console.log(`Сущность не имеет ключа или ИНН, пропускаем:`, entity);
        }
    });

    // console.log("Entities by key: ", entitiesByKey);

    const innArray = Array.from(targetINNs).filter(inn => inn);
    console.log("Целевые ИНН для поиска связей:", innArray);
    console.log("entitiesByKey размер:", entitiesByKey.size);

    if (innArray.length === 0) {
        console.log("Нет ИНН для поиска связей (юр/физ лица).");
        // Даже если нет ИНН, нужно инициализировать connectionsMap для сущностей prevwork
        const connectionsMap = new Map();
        entitiesByKey.forEach((entity, entityKey) => {
            if (!connectionsMap.has(entityKey)) {
                connectionsMap.set(entityKey, {});
            }
        });
        // Обработка prevwork (если она нужна даже без ИНН в других сущностях)
        await processPrevWork(entitiesByKey, connectionsMap);
        return connectionsMap;
    }

    // Инициализируем connectionsMap для всех целевых сущностей по их ключу
    const connectionsMap = new Map();
    entitiesByKey.forEach((entity, entityKey) => {
        if (!connectionsMap.has(entityKey)) {
            connectionsMap.set(entityKey, {});
        }
    });

    // --- ШАГ 2: Обработка сущностей типа 'prevwork' ---
    // (Эта логика может оставаться отдельной, так как она специфична)
    await processPrevWork(entitiesByKey, connectionsMap);

    // --- ШАГ 3: Поиск связей в БД по ИНН ---
    const fullINNQuery = buildINNQuery(innArray);
    // console.log("Выполняемый SQL для ИНН (поиск связей для юр/физ лиц):", fullINNQuery);

    const innRequest = new sql.Request();
    innArray.forEach((inn, index) => {
        innRequest.input(`inn${index}`, sql.VarChar, inn);
    });

    try {
        const innResult = await innRequest.query(fullINNQuery);
        console.log("Количество результатов поиска по ИНН (юр/физ лица):", innResult.recordset.length);

        // --- ШАГ 4: Сопоставление найденных сущностей с целевыми ---
        await mapFoundEntitiesToTargets(innResult.recordset, entitiesByKey, connectionsMap, innArray);

    } catch (err) {
        console.error('Ошибка при поиске связей по ИНН (юр/физ лица):', err);
        throw err; // Пробрасываем ошибку, если критична
    }

    // Выведем итоговый размер connectionsMap и несколько примеров
    console.log(`Итоговый размер connectionsMap (ИНН): ${connectionsMap.size}`);
    // for (const [key, value] of connectionsMap.entries()) {
    //      console.log(`Ключ (entityKey) ${key}: количество групп связей: ${Object.keys(value).length}`);
    //      for (const [groupKey, conns] of Object.entries(value)) {
    //           console.log(`  - для ключа '${groupKey}': ${conns.length} связей`);
    //           // Покажем первые 2, если их много
    //           conns.slice(0, 2).forEach(conn => console.log(`    -> ${conn.connectedEntity.NameShort} (${conn.connectionStatus}, ${conn.connectionDetails})`));
    //           if (conns.length > 2) console.log(`    ... и ещё ${conns.length - 2}`);
    //      }
    // }

    return connectionsMap; // Возвращаем карту связей, сгруппированных по ключу и затем по ИНН
}

// --- ВСПОМОГАТЕЛЬНАЯ ФУНКЦИЯ: Обработка prevwork ---
async function processPrevWork(entitiesByKey, connectionsMap) {
    console.log("Начало обработки prevwork в findConnectionsByINN");
    const prevWorkEntities = Array.from(entitiesByKey.values()).filter(entity => entity.type === 'prevwork' && entity.PersonUNID);
    const personUNIDsToFetch = new Set(prevWorkEntities.map(entity => entity.PersonUNID));

    console.log("Найдены сущности 'prevwork' для обогащения ФИО. PersonUNID:", personUNIDsToFetch);

    if (personUNIDsToFetch.size > 0) {
        console.log(`Найдено ${personUNIDsToFetch.size} уникальных PersonUNID для поиска деталей физ. лиц.`);
        const personDetailsMap = await findPersonDetailsByUNID(Array.from(personUNIDsToFetch));

        // Обновляем connectionsMap, добавляя "связь" с ФИО для каждой сущности 'prevwork'
        // Ключ - это PersonUNID сущности 'prevwork' (которая сама по себе сущность)
        prevWorkEntities.forEach(prevWorkEntity => {
            const entityKey = prevWorkEntity.PersonUNID; // Ключом в connectionsMap будет PersonUNID, к которому привязано prevwork
            const personDetails = personDetailsMap.get(prevWorkEntity.PersonUNID);

            if (personDetails) {
                // Создаём "фиктивную" группу связей по ИНН сущности 'prevwork', чтобы прикрепить ФИО
                const prevWorkINN = prevWorkEntity.INN;
                if (!connectionsMap.get(entityKey)[prevWorkINN]) {
                    connectionsMap.get(entityKey)[prevWorkINN] = [];
                }
                connectionsMap.get(entityKey)[prevWorkINN].push({
                    connectedEntity: {
                        INN: prevWorkEntity.INN, // ИНН организации
                        NameShort: personDetails.NameForDisplay || prevWorkEntity.Caption, // Используем ФИО из personDetails, иначе Caption
                        type: 'physical', // Тип - физлицо
                        sourceTable: 'prevwork', // Откуда пришла информация (из prevwork, но обогащена)
                        // --- ДОБАВЛЯЕМ ИНФОРМАЦИЮ ОБ ИСТОЧНИКЕ ---
                        source: 'local', // Это локальная сущность
                        baseName: prevWorkEntity.baseName, // Берём baseName из исходной сущности prevWork
                        // ---
                        PersonUNID: prevWorkEntity.PersonUNID // Указываем PersonUNID
                    },
                    connectionType: 'person_details_match', // Тип связи - детали физ.лица
                    connectionStatus: 'former_employee', // Статус - бывший сотрудник
                    connectionDetails: `Детали физ.лица из: ${personDetails.sourceTable}, связано с предыдущим местом работы по ИНН ${prevWorkEntity.INN}`
                });
            } else {
                 // Если ФИО не найдено, всё равно можно добавить "связь", просто с Caption
                 const prevWorkINN = prevWorkEntity.INN;
                 if (!connectionsMap.get(entityKey)[prevWorkINN]) {
                     connectionsMap.get(entityKey)[prevWorkINN] = [];
                 }
                 connectionsMap.get(entityKey)[prevWorkINN].push({
                    connectedEntity: {
                        INN: prevWorkEntity.INN,
                        NameShort: prevWorkEntity.Caption, // Используем Caption
                        type: 'physical',
                        sourceTable: 'prevwork',
                        // --- ДОБАВЛЯЕМ ИНФОРМАЦИЮ ОБ ИСТОЧНИКЕ ---
                        source: 'local',
                        baseName: prevWorkEntity.baseName,
                        // ---
                        PersonUNID: prevWorkEntity.PersonUNID
                    },
                    connectionType: 'person_details_match',
                    connectionStatus: 'former_employee',
                    connectionDetails: `Данные из предыдущего места работы по ИНН ${prevWorkEntity.INN}, ФИО не найдено`
                });
            }
        });
    } else {
        console.log("Нет сущностей 'prevwork' для обогащения деталями физических лиц.");
    }
}

// --- ВСПОМОГАТЕЛЬНАЯ ФУНКЦИЯ: Построение SQL-запроса ---
function buildINNQuery(innArray) {
    if (innArray.length === 0) return 'SELECT 1 as dummy WHERE 1=0'; // Пустой запрос, если нет ИНН

    const innParams = innArray.map((inn, index) => `@inn${index}`);

    // Запрос 1: Совпадения в CI_Contragent.INN
    // Запрос 1: Совпадения в CI_Contragent.INN
    const contragentINNQuery = `
        SELECT
            ci.UNID as contactUNID,
            ci.INN as contactINN,
            ci.NameShort as contactNameShort,
            ci.NameFull as contactNameFull,
            ci.UrFiz,
            ci.fIP,
            NULL as fzUID, -- NULL для соответствия UNION
            NULL as cpUID, -- NULL для соответствия UNION
            NULL as PersonUNID, -- NULL для соответствия UNION
            NULL as fzFIO, -- NULL для соответствия UNION
            NULL as phFunction, -- NULL для соответствия UNION
            NULL as phEventType, -- NULL для соответствия UNION
            NULL as phDate, -- NULL для соответствия UNION
            'contragent' as sourceTable,
            ci.UNID as entityKey,
            ci.BaseName as baseName
        FROM CI_Contragent_test ci
        WHERE ci.INN IN (${innParams.join(', ')})
    `;

    // Запрос 3: Совпадения в CI_Employees.phOrgINN
    const employeeINNQuery = `
        SELECT
            ce.phOrgINN as contactUNID,
            ce.phOrgINN as contactINN,
            ce.fzFIO as contactNameShort, -- <<< ИЗМЕНЕНО: Используем fzFIO как NameShort
            ce.fzFIO as contactNameFull,  -- <<< ИЗМЕНЕНО: Используем fzFIO как NameFull
            NULL as UrFiz, -- NULL для соответствия UNION
            NULL as fIP,   -- NULL для соответствия UNION
            ce.fzUID as fzUID,
            NULL as cpUID, -- NULL для соответствия UNION
            NULL as PersonUNID, -- NULL для соответствия UNION
            ce.fzFIO as fzFIO, -- <<< ДОБАВЛЕНО: Поле fzFIO
            ce.phFunction as phFunction, -- <<< ДОБАВЛЕНО: Поле должности
            ce.phEventType as phEventType, -- <<< ДОБАВЛЕНО: Поле события
            ce.phDate as phDate, -- <<< ДОБАВЛЕНО: Поле даты
            'employee' as sourceTable,
            ce.fzUID as entityKey,
            ce.BaseName as baseName
        FROM CI_Employees_test ce
        WHERE ce.phOrgINN IN (${innParams.join(', ')})
    `;

    // Запрос 4: Совпадения в CI_ContPersons.conINN
    const contPersonINNQuery = `
        SELECT
            cip.conINN as contactUNID,
            cip.conINN as contactINN,
            cip.cpNameFull as contactNameShort,
            cip.cpNameFull as contactNameFull,
            NULL as UrFiz, -- NULL для соответствия UNION
            NULL as fIP,   -- NULL для соответствия UNION
            NULL as fzUID, -- NULL для соответствия UNION
            cip.cpUID as cpUID,
            NULL as PersonUNID, -- NULL для соответствия UNION
            NULL as fzFIO, -- NULL для соответствия UNION
            NULL as phFunction, -- NULL для соответствия UNION
            NULL as phEventType, -- NULL для соответствия UNION
            NULL as phDate, -- NULL для соответствия UNION
            'contperson' as sourceTable,
            cip.cpUID as entityKey,
            cip.BaseName as baseName
        FROM CI_ContPersons_test cip
        WHERE cip.conINN IN (${innParams.join(', ')})
    `;


    return `${contragentINNQuery}  UNION ALL ${employeeINNQuery} UNION ALL ${contPersonINNQuery}`;
}

// --- ВСПОМОГАТЕЛЬНАЯ ФУНКЦИЯ: Сопоставление найденных сущностей с целевыми ---
async function mapFoundEntitiesToTargets(foundRecordset, entitiesByKey, connectionsMap, targetINNs) {
    console.log("Начало сопоставления найденных сущностей с целевыми");
    console.log('connectionMap: ', connectionsMap);
    foundRecordset.forEach(row => {
        // Определяем тип и ключ найденной сущности (connectedEntity)
        let connectedType = 'unknown';
        // connectedName теперь может быть ФИО, если это сотрудник
        let connectedName = row.contactNameShort || row.contactNameFull || row.contactINN || 'N/A';
        let connectedEntityKey = row.entityKey; // Уникальный ключ из результата SQL
        let connectionStatus = 'unknown_status';
        let baseName = row.baseName || null;

        if (!connectedEntityKey) {
            console.log(`Предупреждение: Не удалось определить ключ для найденной сущности из ${row.sourceTable}. Пропускаем.`, row);
            return;
        }

        // Извлекаем дополнительные поля сотрудника (могут быть null)
        let fzFIO = row.fzFIO || null;
        let phFunction = row.phFunction || null;
        let phEventType = row.phEventType || null;
        let phDate = row.phDate ? new Date(row.phDate).toLocaleDateString() : null;

        // Определяем тип и статус
        if (row.sourceTable === 'contragent') {
            connectedType = determineEntityType(row.UrFiz, row.fIP);
            connectionStatus = 'organization_match';
        } else if (row.sourceTable === 'prevwork') {
            connectedType = 'physical';
            connectionStatus = 'former_employee';
        } else if (row.sourceTable === 'employee') { // <<< Обработка 'employee'
            connectedType = 'physical';
            // connectionStatus определяется по phEventType, как раньше
            connectionStatus = (phEventType && phEventType.toLowerCase().includes('увол')) ? 'former_employee' : 'current_employee';
        } else if (row.sourceTable === 'contperson') {
            connectedType = 'physical';
            connectionStatus = 'contact_person';
        }

        const foundINN = row.contactINN;

        if (!targetINNs.includes(foundINN)) {
             console.log(`Найденная сущность ${connectedEntityKey} имеет ИНН '${foundINN}', но оно не является целевым. Пропускаем.`);
             return;
        }

        entitiesByKey.forEach((targetEntity, targetEntityKey) => {
            if (targetEntity.type === 'prevwork') {
                 return;
            }

            if (targetEntity.INN === foundINN) {
                if (connectedEntityKey !== targetEntityKey) {
                    if (!connectionsMap.get(targetEntityKey)[foundINN]) {
                        connectionsMap.get(targetEntityKey)[foundINN] = [];
                    }

                    // Подготовим строку с деталями сотрудника, если это тип 'employee'
                    let employeeInfo = null; // Инициализируем null
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
                        connectionDetails: `Совпадение по ИНН: ${foundINN}, найдено в таблице ${row.sourceTable}, статус: ${connectionStatus}`,
                        // <<< ДОБАВЛЕНО: Поле с деталями сотрудника >>>
                        employeeInfo: employeeInfo
                    });
                }
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