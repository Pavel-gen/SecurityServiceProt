import sql from 'mssql'
import { determineEntityType } from '../utils/helper.js';

async function findConnectionsByINN(targetEntities, request) {
    // Собираем уникальные ИНН из целевых сущностей
    const targetINNs = new Set();
    const entitiesByKey = new Map(); // Используем ключ (UNID, fzUID, cpUID, PersonUNID)

    targetEntities.forEach(entity => {
        const entityKey = entity.UNID || entity.fzUID || entity.cpUID || entity.PersonUNID;
        if (entityKey && entity.INN && entity.INN.trim() !== '') {
            targetINNs.add(entity.INN);
            entitiesByKey.set(entityKey, entity);
        }
    });

    const innArray = Array.from(targetINNs).filter(inn => inn);

    console.log("Целевые ИНН для поиска связей:", innArray);

    const connectionsMap = new Map();
    // Инициализируем connectionsMap для всех целевых сущностей по их ключу
    entitiesByKey.forEach((entity, entityKey) => {
        if (!connectionsMap.has(entityKey)) {
            connectionsMap.set(entityKey, {});
        }
    });

    // --- НОВЫЙ БЛОК: Обработка сущностей типа 'prevwork' ---
    // Цель: Найти ФИО для PersonUNID, содержащихся в сущностях 'prevwork'
    const prevWorkEntities = targetEntities.filter(entity => entity.type === 'prevwork' && entity.PersonUNID);
    const personUNIDsToFetch = new Set(prevWorkEntities.map(entity => entity.PersonUNID));

    console.log("Найдены сущности 'prevwork' для обогащения ФИО. PersonUNID:", personUNIDsToFetch);

    if (personUNIDsToFetch.size > 0) {
        console.log(`Найдено ${personUNIDsToFetch.size} уникальных PersonUNID для поиска деталей физ. лиц.`);
        const personDetailsMap = await findPersonDetailsByUNID(Array.from(personUNIDsToFetch));

        // Обновляем connectionsMap, добавляя "связь" с ФИО для каждой сущности 'prevwork'
        // Ключ - это PersonUNID сущности 'prevwork'
        prevWorkEntities.forEach(prevWorkEntity => {
            const entityKey = prevWorkEntity.PersonUNID;
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


    // --- СТАРЫЙ БЛОК: Поиск связей по ИНН для остальных сущностей (contragent, employee, contperson) ---
    // (Остальная часть функции, если нужно искать связи по ИНН для юр/физ лиц)
    // Этот блок может остаться, если вы хотите, чтобы findConnectionsByINN также искала
    // другие сущности (contragent, employee, contperson) с тем же ИНН, как описано выше.
    // Если только для prevwork, то оставляем только новый блок.

    if (innArray.length > 0) {
        // Подготовим параметры для WHERE - ищем точное совпадение ИНН
        const innParams = innArray.map((inn, index) => `@inn${index}`);

        // Запрос 1: Совпадения в CI_Contragent.INN
        const contragentINNQuery = `
            SELECT
                ci.UNID as contactUNID,
                ci.INN as contactINN,
                ci.NameShort as contactNameShort,
                ci.NameFull as contactNameFull,
                ci.UrFiz,
                ci.fIP,
                NULL as fzUID,
                NULL as cpUID,
                NULL as PersonUNID,
                'contragent' as sourceTable,
                ci.UNID as entityKey,
                ci.BaseName as baseName -- Добавляем BaseName
            FROM CI_Contragent ci
            WHERE ci.INN IN (${innParams.join(', ')})
        `;

        // Запрос 2: Совпадения в CF_PrevWork.INN (не для целей поиска ФИО, а как потенциальные связи)
        const prevWorkINNQuery = `
            SELECT
                NULL as contactUNID,
                cpw.INN as contactINN,
                cpw.Caption as contactNameShort,
                cpw.Caption as contactNameFull,
                NULL as UrFiz,
                NULL as fIP,
                NULL as fzUID,
                NULL as cpUID,
                cpw.PersonUNID as PersonUNID, -- Ключ к физ. лицу
                'prevwork' as sourceTable,
                cpw.PersonUNID as entityKey, -- Используем PersonUNID как ключ
                NULL as baseName -- BaseName нет в CF_PrevWork
            FROM CF_PrevWork cpw
            WHERE cpw.INN IN (${innParams.join(', ')})
        `;

        // Запрос 3: Совпадения в CI_Employees.phOrgINN
        const employeeINNQuery = `
            SELECT
                ce.phOrgINN as contactUNID,
                ce.phOrgINN as contactINN,
                ce.phOrgName as contactNameShort,
                ce.phOrgName as contactNameFull,
                NULL as UrFiz,
                NULL as fIP,
                ce.fzUID as fzUID,
                NULL as cpUID,
                NULL as PersonUNID,
                'employee' as sourceTable,
                ce.fzUID as entityKey,
                ce.BaseName as baseName -- Добавляем BaseName
            FROM CI_Employees ce
            WHERE ce.phOrgINN IN (${innParams.join(', ')})
        `;

        // Запрос 4: Совпадения в CI_ContPersons.conINN
        const contPersonINNQuery = `
            SELECT
                cip.conINN as contactUNID,
                cip.conINN as contactINN,
                cip.cpNameFull as contactNameShort,
                cip.cpNameFull as contactNameFull,
                NULL as UrFiz,
                NULL as fIP,
                NULL as fzUID,
                cip.cpUID as cpUID,
                NULL as PersonUNID,
                'contperson' as sourceTable,
                cip.cpUID as entityKey,
                cip.BaseName as baseName -- Добавляем BaseName
            FROM CI_ContPersons cip
            WHERE cip.conINN IN (${innParams.join(', ')})
        `;

        const fullINNQuery = `${contragentINNQuery} UNION ALL ${prevWorkINNQuery} UNION ALL ${employeeINNQuery} UNION ALL ${contPersonINNQuery}`;

        console.log("Выполняемый SQL для ИНН (поиск связей для юр/физ лиц):", fullINNQuery);

        const innRequest = new sql.Request();
        innArray.forEach((inn, index) => {
            innRequest.input(`inn${index}`, sql.VarChar, inn);
        });

        try {
            const innResult = await innRequest.query(fullINNQuery);
            console.log("Количество результатов поиска по ИНН (юр/физ лица):", innResult.recordset.length);

            // --- СОПОСТАВЛЕНИЕ НАЙДЕННЫХ СУЩНОСТЕЙ С ЦЕЛЕВЫМИ (юр/физ лица) ---
            innResult.recordset.forEach(row => {
                // Определяем тип и ключ найденной сущности (connectedEntity)
                let connectedType = 'unknown';
                let connectedName = row.contactNameShort || row.contactNameFull || row.contactINN || 'N/A';
                let connectedEntityKey = row.entityKey; // Уникальный ключ из результата SQL
                let connectionStatus = 'unknown_status'; // Статус связи: например, 'former_employee', 'current_employee', 'contact_person'
                let baseName = row.baseName || null; // Имя базы данных источника

                if (!connectedEntityKey) {
                    console.log(`Предупреждение: Не удалось определить ключ для найденной сущности из ${row.sourceTable}. Пропускаем.`, row);
                    return;
                }

                // Определяем тип
                if (row.sourceTable === 'contragent') {
                    connectedType = determineEntityType(row.UrFiz, row.fIP);
                    connectionStatus = 'organization_match'; // Совпадение по ИНН организации
                } else if (row.sourceTable === 'prevwork') {
                    connectedType = 'physical'; // Это физлицо из предыдущего места работы
                    connectionStatus = 'former_employee'; // Бывший сотрудник
                } else if (row.sourceTable === 'employee') {
                    connectedType = 'physical'; // Предполагаем, что это физлицо - сотрудник
                    connectionStatus = (row.phEventType && row.phEventType.toLowerCase().includes('увол')) ? 'former_employee' : 'current_employee';
                } else if (row.sourceTable === 'contperson') {
                    connectedType = 'physical'; // Предполагаем, что это физлицо - контактное лицо
                    connectionStatus = 'contact_person';
                }

                const foundINN = row.contactINN;

                // Проверяем, содержится ли найденное ИНН в целевых ИНН
                if (!innArray.includes(foundINN)) {
                     console.log(`Найденная сущность ${connectedEntityKey} имеет ИНН '${foundINN}', но оно не является целевым. Пропускаем.`);
                     return;
                }

                // console.log(`Обработка найденной сущности: ${connectedEntityKey}, INN: ${foundINN}, Name: ${connectedName}`); // Лог

                // Перебираем ВСЕ целевые сущности
                entitiesByKey.forEach((targetEntity, targetEntityKey) => {
                    // Пропускаем сущности типа 'prevwork' в этом цикле, чтобы не ломать логику
                    if (targetEntity.type === 'prevwork') {
                         return; // Переходим к следующей итерации
                    }

                    // Проверяем, что целевая сущность имеет ИНН и оно совпадает с найденным
                    if (targetEntity.INN === foundINN) {
                        // Проверяем, что найденная сущность не является целевой (по ключу)
                        if (connectedEntityKey !== targetEntityKey) {
                            // console.log(`Совпадение найдено: целевая сущность ${targetEntityKey}, найденная сущность ${connectedEntityKey}`); // Лог

                            if (!connectionsMap.get(targetEntityKey)[foundINN]) {
                                connectionsMap.get(targetEntityKey)[foundINN] = [];
                            }
                            // Лог перед добавлением
                            // console.log(`Добавление связи по ИНН: целевая ${targetEntityKey} -> найденная ${connectedEntityKey}, ИНН ${foundINN}`); // Лог
                            connectionsMap.get(targetEntityKey)[foundINN].push({
                                connectedEntity: {
                                    INN: row.contactINN,
                                    NameShort: connectedName, // Может быть NameShort/Full org или Caption
                                    type: connectedType,
                                    sourceTable: row.sourceTable, // Откуда пришла информация
                                    // --- ДОБАВЛЯЕМ ИНФОРМАЦИЮ ОБ ИСТОЧНИКЕ ---
                                    source: 'local', // Это локальная сущность из БД
                                    baseName: baseName, // Добавляем имя базы данных источника
                                    // ---
                                    PersonUNID: row.PersonUNID // Добавляем PersonUNID, если он есть (например, для prevwork)
                                },
                                connectionType: 'inn_match',
                                connectionStatus: connectionStatus, // Добавляем статус
                                connectionDetails: `Совпадение по ИНН: ${foundINN}, найдено в таблице ${row.sourceTable}, статус: ${connectionStatus}`
                            });
                        } else {
                            // console.log(`Пропуск: целевая и найденная сущности совпадают по ключу ${targetEntityKey}`); // Лог
                        }
                    }
                });
            });

        } catch (err) {
            console.error('Ошибка при поиске связей по ИНН (юр/физ лица):', err);
            throw err; // Пробрасываем ошибку, если критична
        }
    } else {
        console.log("Нет ИНН для поиска связей (юр/физ лица).");
    }


    // Выведем итоговый размер connectionsMap и несколько примеров
    console.log(`Итоговый размер connectionsMap (ИНН): ${connectionsMap.size}`);
    for (const [key, value] of connectionsMap.entries()) {
        console.log(`Ключ (entityKey) ${key}: количество групп связей: ${Object.keys(value).length}`);
        for (const [groupKey, conns] of Object.entries(value)) {
             console.log(`  - для ключа '${groupKey}': ${conns.length} связей`);
             // Покажем первые 2, если их много
             conns.slice(0, 2).forEach(conn => console.log(`    -> ${conn.connectedEntity.NameShort} (${conn.connectionStatus}, ${conn.connectionDetails})`));
             if (conns.length > 2) console.log(`    ... и ещё ${conns.length - 2}`);
        }
    }

    return connectionsMap; // Возвращаем карту связей, сгруппированных по ключу и затем по ИНН
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

    console.log("Выполняемый SQL для поиска деталей физ. лиц:", fullPersonQuery);

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