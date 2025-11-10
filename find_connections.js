// find_connections.js
const sql = require('mssql');

// --- ВСПОМОГАТЕЛЬНАЯ ФУНКЦИЯ: Определение типа сущности ---
function determineEntityType(UrFiz, fIP) {
    if (fIP === 1) return 'ip';
    if (UrFiz === 1) return 'juridical';
    if (UrFiz === 2) return 'physical';
    return 'unknown';
}

function cleanPhone(phone) {
    // Убираем все нецифровые символы, кроме +
    return phone.replace(/[^\d+]/g, '');
}

// --- ОСНОВНАЯ ФУНКЦИЯ: Поиск связей по email ---
async function findConnectionsByEmail(targetEntities, request) {
    // Собираем уникальные email из целевых сущностей
    const targetEmails = new Set();
    const entitiesByKey = new Map(); // Используем ключ (UNID или fzUID)

    targetEntities.forEach(entity => {
        const entityKey = entity.UNID || entity.fzUID; // Новый ключ: UNID (для CI_Contragent) или fzUID (для CI_Employees)
        if (entityKey && entity.eMail && entity.eMail.trim() !== '') { // Проверяем наличие ключа
            const emails = entity.eMail.toLowerCase().split(';').map(email => email.trim()).filter(email => email);
            console.log(`Обработка целевой сущности: ${entityKey}, eMail: ${entity.eMail}, Name: ${entity.NameShort || entity.fzFIO}`); // Лог
            emails.forEach(email => targetEmails.add(email));
            entitiesByKey.set(entityKey, entity); // Сохраняем по новому ключу
        }
    });

    const emailArray = Array.from(targetEmails).filter(email => email);

    console.log("Целевые email для поиска связей:", emailArray); // Лог

    const connectionsMap = new Map();
    // Инициализируем connectionsMap для всех целевых сущностей по их ключу
    entitiesByKey.forEach((entity, entityKey) => {
        if (!connectionsMap.has(entityKey)) {
            connectionsMap.set(entityKey, {});
            console.log(`Инициализирована карта связей для ключа: ${entityKey}`); // Лог
        }
    });

    if (emailArray.length > 0) {
        // Подготовим условия для WHERE - ищем точное совпадение email
        let emailORCondition = '';
        emailArray.forEach((email, index) => {
            emailORCondition += `(LOWER(ci.eMail) LIKE @email${index})`;
            if (index < emailArray.length - 1) emailORCondition += ' OR ';
        });

        // Запрос 1: Совпадения в CI_Contragent_test.eMail
        const contragentEmailQuery = `
            SELECT
                ci.UNID as contactUNID, -- Получаем UNID вместо INN
                ci.INN as contactINN, -- Оставим INN для отображения
                ci.NameShort as contactNameShort,
                ci.NameFull as contactNameFull,
                ci.eMail as contactEmail,
                ci.UrFiz,
                ci.fIP,
                NULL as fzUID -- У юрлиц/ип/физлиц из CI_Contragent fzUID нет
            FROM CI_Contragent_test ci
            WHERE ${emailORCondition}
        `;

        // Запрос 2: Совпадения в CI_Employees_test.fzMail
        const employeeEmailQuery = `
            SELECT
                NULL as contactUNID, -- У сотрудника нет UNID из CI_Contragent
                NULL as contactINN, -- ИНН организации не возвращаем, если не делаем JOIN или он не нужен
                ce.fzFIO as contactNameShort,
                ce.fzFIO as contactNameFull,
                ce.fzMail as contactEmail,
                2 as UrFiz, -- Предполагаем, что сотрудник - физлицо
                0 as fIP,   -- Предполагаем, что сотрудник - не ИП (в рамках нашей логики отображения)
                ce.fzUID as fzUID -- fzUID сотрудника
            FROM CI_Employees_test ce
            WHERE LOWER(ce.fzMail) IN (${emailArray.map((_, idx) => `@email${idx}`).join(', ')}) -- Точное совпадение
        `;

        const fullEmailQuery = `${contragentEmailQuery} UNION ALL ${employeeEmailQuery}`;

        console.log("Выполняемый SQL для email:", fullEmailQuery); // Лог

        const emailRequest = new sql.Request();
        emailArray.forEach((email, index) => {
            emailRequest.input(`email${index}`, sql.VarChar, email); // Передаем просто email для точного совпадения
        });

        try {
            const emailResult = await emailRequest.query(fullEmailQuery);
            console.log("Количество результатов поиска по email:", emailResult.recordset.length); // Лог
            // Выведем первые несколько результатов для проверки
            console.log("Примеры найденных сущностей:", emailResult.recordset.slice(0, 5)); // Лог

            emailResult.recordset.forEach(row => {
                const connectedType = determineEntityType(row.UrFiz, row.fIP);
                let connectedName = row.contactNameShort || row.contactNameFull || 'N/A';
                const foundEmail = row.contactEmail;

                const foundEmails = foundEmail.toLowerCase().split(';').map(email => email.trim()).filter(email => email);
                const matchingTargetEmails = foundEmails.filter(found_addr => emailArray.includes(found_addr));

                if (matchingTargetEmails.length === 0) {
                     console.log(`Найденная сущность ${row.contactUNID || row.fzUID} имеет email '${foundEmail}', но не содержит целевых emails. Пропускаем.`);
                     return;
                }

                // console.log(`Обработка найденной сущности: ${row.contactUNID || row.fzUID}, eMail: ${foundEmail}, Name: ${connectedName}`); // Лог

                // Находим целевые сущности, у которых есть этот email
                entitiesByKey.forEach((targetEntity, targetEntityKey) => {
                    const targetEmailsList = targetEntity.eMail.toLowerCase().split(';').map(email => email.trim()).filter(email => email);
                    const matchingTargetEmailsInEntity = targetEmailsList.filter(target_addr => emailArray.includes(target_addr));

                    const connectedEntityKey = row.contactUNID || row.fzUID;

                    // Проверяем, что найденная сущность не является целевой (по ключу)
                    if (matchingTargetEmailsInEntity.length > 0 && connectedEntityKey !== targetEntityKey) {
                        // console.log(`Совпадение найдено: целевая сущность ${targetEntityKey}, найденная сущность ${connectedEntityKey}`); // Лог
                        const intersection = matchingTargetEmails.filter(addr => targetEmailsList.includes(addr));
                        if (intersection.length > 0) {
                            intersection.forEach(intersectingEmail => {
                                if (!connectionsMap.get(targetEntityKey)[intersectingEmail]) {
                                    connectionsMap.get(targetEntityKey)[intersectingEmail] = [];
                                }
                                // Лог перед добавлением
                                // console.log(`Добавление связи: целевая ${targetEntityKey} -> найденная ${connectedEntityKey}, email ${intersectingEmail}`); // Лог
                                connectionsMap.get(targetEntityKey)[intersectingEmail].push({
                                    connectedEntity: {
                                        INN: row.contactINN, // ИНН связанной сущности (если есть)
                                        NameShort: connectedName,
                                        type: connectedType
                                    },
                                    connectionType: 'email_match',
                                    connectionDetails: `Совпадение по email: ${intersectingEmail}`
                                });
                            });
                        }
                    } else {
                        if (connectedEntityKey === targetEntityKey) {
                            // console.log(`Пропуск: целевая и найденная сущности совпадают по ключу ${targetEntityKey}`); // Лог
                        }
                    }
                });
            });
        } catch (err) {
            console.error('Ошибка при поиске связей по email:', err);
            throw err;
        }
    } else {
        console.log("Нет email для поиска связей.");
    }

    // Выведем итоговый размер connectionsMap и несколько примеров
    console.log(`Итоговый размер connectionsMap: ${connectionsMap.size}`);
    for (const [key, value] of connectionsMap.entries()) {
        console.log(`Ключ ${key}: количество email-групп связей: ${Object.keys(value).length}`);
        // Покажем количество связей в первой группе, если есть
        const firstEmailKey = Object.keys(value)[0];
        if (firstEmailKey) {
            console.log(`  - для email '${firstEmailKey}': ${value[firstEmailKey].length} связей`);
        }
    }

    return connectionsMap; // Возвращаем карту связей, сгруппированных по ключу (UNID или fzUID) и затем по email
}


// ... (ваш существующий код find_connections.js до findConnectionsByPhone) ...

// --- ОСНОВНАЯ ФУНКЦИЯ: Поиск связей по телефону ---
async function findConnectionsByPhone(targetEntities, request) {
    // Собираем уникальные телефоны из целевых сущностей
    const targetPhones = new Set();
    const entitiesByKey = new Map(); // Используем ключ (UNID или fzUID или cpUID)

    targetEntities.forEach(entity => {
        const entityKey = entity.UNID || entity.fzUID || entity.cpUID;
        if (entityKey) {
            // Собираем телефоны из всех возможных полей
            const phones = [];
            if (entity.PhoneNum) phones.push(...entity.PhoneNum.split(';').map(phone => phone.trim()).filter(phone => phone));
            if (entity.fzPhoneM) phones.push(...entity.fzPhoneM.split(';').map(phone => phone.trim()).filter(phone => phone));
            if (entity.cpPhoneMob) phones.push(...entity.cpPhoneMob.split(';').map(phone => phone.trim()).filter(phone => phone));
            if (entity.cpPhoneWork) phones.push(...entity.cpPhoneWork.split(';').map(phone => phone.trim()).filter(phone => phone));

            phones.forEach(phone => {
                const cleanedPhone = cleanPhone(phone);
                if (cleanedPhone) targetPhones.add(cleanedPhone);
            });
            entitiesByKey.set(entityKey, entity);
        }
    });

    const phoneArray = Array.from(targetPhones).filter(phone => phone);

    console.log("Целевые телефоны для поиска связей:", phoneArray); // Лог

    const connectionsMap = new Map();
    // Инициализируем connectionsMap для всех целевых сущностей по их ключу
    entitiesByKey.forEach((entity, entityKey) => {
        if (!connectionsMap.has(entityKey)) {
            connectionsMap.set(entityKey, {});
        }
    });

    if (phoneArray.length > 0) {
        // Подготовим условия LIKE для каждого телефона как параметры
        const phoneParams = phoneArray.map((phone, index) => `@phone${index}`);

        // --- КОНСТРУИРОВАНИЕ ДИНАМИЧЕСКОГО SQL БЕЗ buildPhoneCondition ---
        // Вручную формируем условия WHERE для каждой таблицы, используя правильные столбцы
        // Убираем условие phOrgINN из employeePhoneQuery для поиска сотрудников по fzPhoneM/fzPhone без привязки к org

        // Условие для CI_Contragent_test (ищем в PhoneNum)
        const contragentWhereConditions = phoneParams.map(param => `REPLACE(REPLACE(REPLACE(REPLACE(ci.PhoneNum, ' ', ''), '-', ''), '(', ''), ')', '') LIKE ${param}`).join(' OR ');

        // Условие для CI_Employees_test (ищем в fzPhoneM, fzPhone - без условия phOrgINN)
        const employeeWhereConditions = phoneParams.map(param => `(REPLACE(REPLACE(REPLACE(REPLACE(ce.fzPhoneM, ' ', ''), '-', ''), '(', ''), ')', '') LIKE ${param} OR REPLACE(REPLACE(REPLACE(REPLACE(ce.fzPhone, ' ', ''), '-', ''), '(', ''), ')', '') LIKE ${param})`).join(' OR ');

        // Условие для CI_ContPersons_test (ищем в cpPhoneMob, cpPhoneWork)
        const contPersonWhereConditions = phoneParams.map(param => `(REPLACE(REPLACE(REPLACE(REPLACE(cip.cpPhoneMob, ' ', ''), '-', ''), '(', ''), ')', '') LIKE ${param} OR REPLACE(REPLACE(REPLACE(REPLACE(cip.cpPhoneWork, ' ', ''), '-', ''), '(', ''), ')', '') LIKE ${param})`).join(' OR ');

        // Условие для CF_PrevWork_test (ищем в Phone)
        const prevWorkWhereConditions = phoneParams.map(param => `REPLACE(REPLACE(REPLACE(REPLACE(cpw.Phone, ' ', ''), '-', ''), '(', ''), ')', '') LIKE ${param}`).join(' OR ');

        const fullPhoneQuery = `
            -- Запрос 1: Совпадения в CI_Contragent_test.PhoneNum
            SELECT
                ci.UNID as contactUNID,
                ci.INN as contactINN,
                ci.NameShort as contactNameShort,
                ci.NameFull as contactNameFull,
                ci.PhoneNum as contactPhone,
                ci.UrFiz,
                ci.fIP,
                NULL as fzUID,
                NULL as cpUID,
                NULL as PersonUNID,
                'contragent' as sourceTable,
                ci.UNID as entityKey -- Уникальный ключ для сущности
            FROM CI_Contragent_test ci
            WHERE (${contragentWhereConditions}) -- Условия для ci.PhoneNum

            UNION ALL

            -- Запрос 2: Совпадения в CI_Employees_test.fzPhoneM или fzPhone (БЕЗ условия phOrgINN)
            SELECT
                ce.phOrgINN as contactUNID, -- ИНН организации как "ключ" для сотрудника (может быть NULL)
                ce.phOrgINN as contactINN,
                ce.fzFIO as contactNameShort,
                ce.fzFIO as contactNameFull,
                ISNULL(ce.fzPhoneM, ce.fzPhone) as contactPhone, -- Берем fzPhoneM или fzPhone
                NULL as UrFiz,
                NULL as fIP,
                ce.fzUID as fzUID, -- fzUID сотрудника
                NULL as cpUID,
                NULL as PersonUNID,
                'employee' as sourceTable,
                ce.fzUID as entityKey -- fzUID как уникальный ключ
            FROM CI_Employees_test ce
            WHERE (${employeeWhereConditions}) -- Условия для ce.fzPhoneM и ce.fzPhone, БЕЗ phOrgINN

            UNION ALL

            -- Запрос 3: Совпадения в CI_ContPersons_test.cpPhoneMob или cpPhoneWork
            SELECT
                cip.conINN as contactUNID, -- ИНН организации
                cip.conINN as contactINN,
                cip.cpNameFull as contactNameShort,
                cip.cpNameFull as contactNameFull,
                ISNULL(cip.cpPhoneMob, cip.cpPhoneWork) as contactPhone, -- Берем один из телефонов
                NULL as UrFiz,
                NULL as fIP,
                NULL as fzUID,
                cip.cpUID as cpUID, -- cpUID контактного лица
                NULL as PersonUNID,
                'contperson' as sourceTable,
                cip.cpUID as entityKey -- cpUID как уникальный ключ
            FROM CI_ContPersons_test cip
            WHERE (${contPersonWhereConditions}) -- Условия для cip.cpPhoneMob/Work

            UNION ALL

            -- Запрос 4: Совпадения в CF_PrevWork_test.Phone
            SELECT
                NULL as contactUNID,
                cpw.INN as contactINN,
                cpw.Caption as contactNameShort,
                cpw.Caption as contactNameFull,
                cpw.Phone as contactPhone,
                NULL as UrFiz,
                NULL as fIP,
                NULL as fzUID,
                NULL as cpUID,
                cpw.PersonUNID as PersonUNID,
                'prevwork' as sourceTable,
                cpw.PersonUNID as entityKey -- PersonUNID как уникальный ключ
            FROM CF_PrevWork_test cpw
            WHERE (${prevWorkWhereConditions}) -- Условия для cpw.Phone
        `;

        console.log("Выполняемый SQL для телефона:", fullPhoneQuery); // Лог

        // Создаем запрос и добавляем параметры для всех телефонов
        const phoneRequest = new sql.Request();
        phoneArray.forEach((phone, index) => {
            phoneRequest.input(`phone${index}`, sql.VarChar, `%${phone}%`);
        });

        try {
            const phoneResult = await phoneRequest.query(fullPhoneQuery);
            console.log("Количество результатов поиска по телефону:", phoneResult.recordset.length); // Лог

            // --- СОПОСТАВЛЕНИЕ НАЙДЕННЫХ СУЩНОСТЕЙ С ЦЕЛЕВЫМИ ---
            phoneResult.recordset.forEach(row => {
                // Определяем тип и ключ найденной сущности (connectedEntity)
                let connectedType = 'unknown';
                let connectedName = row.contactNameShort || row.contactNameFull || 'N/A';
                let connectedEntityKey = row.entityKey; // Уникальный ключ из результата SQL

                if (!connectedEntityKey) {
                    console.log(`Предупреждение: Не удалось определить ключ для найденной сущности из ${row.sourceTable}. Пропускаем.`, row);
                    return;
                }

                // Определяем тип
                if (row.sourceTable === 'contragent') {
                    connectedType = determineEntityType(row.UrFiz, row.fIP);
                } else if (row.sourceTable === 'employee') {
                    connectedType = 'physical';
                } else if (row.sourceTable === 'contperson') {
                    connectedType = 'physical';
                } else if (row.sourceTable === 'prevwork') {
                    connectedType = 'physical';
                }

                const foundPhone = row.contactPhone;
                const foundPhones = foundPhone.split(';').map(phone => phone.trim()).filter(phone => phone);
                const foundCleanedPhones = foundPhones.map(cleanPhone).filter(phone => phone);
                // НЕ проверяем, содержится ли foundPhone в targetPhones, т.к. мы уже искали по targetPhones
                // Вместо этого, проверим, есть ли пересечение с телефонами целевой сущности

                // Перебираем ВСЕ целевые сущности
                entitiesByKey.forEach((targetEntity, targetEntityKey) => {
                    // Собираем телефоны целевой сущности
                    const targetPhonesList = [];
                    if (targetEntity.PhoneNum) targetPhonesList.push(...targetEntity.PhoneNum.split(';').map(p => p.trim()).filter(p => p));
                    if (targetEntity.fzPhoneM) targetPhonesList.push(...targetEntity.fzPhoneM.split(';').map(p => p.trim()).filter(p => p));
                    if (targetEntity.cpPhoneMob) targetPhonesList.push(...targetEntity.cpPhoneMob.split(';').map(p => p.trim()).filter(p => p));
                    if (targetEntity.cpPhoneWork) targetPhonesList.push(...targetEntity.cpPhoneWork.split(';').map(p => p.trim()).filter(p => p));

                    const targetCleanedPhones = targetPhonesList.map(cleanPhone).filter(phone => phone);
                    // Проверяем пересечение телефонов
                    const matchingPhones = foundCleanedPhones.filter(phone => targetCleanedPhones.includes(phone));

                    // Проверяем, что найденная сущность не является целевой и есть совпадение по телефону
                    if (connectedEntityKey !== targetEntityKey && matchingPhones.length > 0) {
                        // Для каждого совпавшего телефона добавляем связь
                        matchingPhones.forEach(matchingPhone => {
                            if (!connectionsMap.get(targetEntityKey)[matchingPhone]) {
                                connectionsMap.get(targetEntityKey)[matchingPhone] = [];
                            }
                            // Проверяем, нет ли уже такой связи (защита от дубликатов при пересечении условий)
                            const existingConnection = connectionsMap.get(targetEntityKey)[matchingPhone].find(conn => conn.connectedEntity.INN === row.contactINN && conn.connectedEntity.NameShort === connectedName);
                            if (!existingConnection) {
                                console.log(`Добавление связи по телефону: целевая ${targetEntityKey} -> найденная ${connectedEntityKey} (из ${row.sourceTable}), телефон ${matchingPhone}`); // Лог
                                connectionsMap.get(targetEntityKey)[matchingPhone].push({
                                    connectedEntity: {
                                        INN: row.contactINN,
                                        NameShort: connectedName,
                                        type: connectedType
                                    },
                                    connectionType: 'phone_match',
                                    connectionDetails: `Совпадение по телефону: ${matchingPhone}`
                                });
                            } else {
                                console.log(`Связь уже существует: целевая ${targetEntityKey} -> найденная ${connectedEntityKey}, телефон ${matchingPhone}`); // Лог
                            }
                        });
                    } else {
                        if (connectedEntityKey === targetEntityKey) {
                            console.log(`Пропуск: целевая и найденная сущности совпадают по ключу ${targetEntityKey} (телефон)`); // Лог
                        }
                        // else if (matchingPhones.length === 0) { console.log(`Пропуск: нет совпадения по телефону между целевой ${targetEntityKey} и найденной ${connectedEntityKey}`); }
                    }
                });
            });

        } catch (err) {
            console.error('Ошибка при поиске связей по телефону:', err);
            throw err;
        }
    } else {
        console.log("Нет телефонов для поиска связей.");
    }

    // Выведем итоговый размер connectionsMap и несколько примеров
    console.log(`Итоговый размер connectionsMap (телефон): ${connectionsMap.size}`);
    for (const [key, value] of connectionsMap.entries()) {
        console.log(`Ключ (телефон) ${key}: количество phone-групп связей: ${Object.keys(value).length}`);
        for (const [phoneKey, conns] of Object.entries(value)) {
             console.log(`  - для телефона '${phoneKey}': ${conns.length} связей`);
             // Покажем первые 2, если их много
             conns.slice(0, 2).forEach(conn => console.log(`    -> ${conn.connectedEntity.NameShort} (${conn.connectionDetails})`));
             if (conns.length > 2) console.log(`    ... и ещё ${conns.length - 2}`);
        }
    }

    return connectionsMap; // Возвращаем карту связей, сгруппированных по ключу и затем по телефону
}

// find_connections.js

// ... (ваш существующий код до функции findConnectionsByPhone) ...

// --- ОСНОВНАЯ ФУНКЦИЯ: Поиск связей по ИНН ---
// --- ОСНОВНАЯ ФУНКЦИЯ: Поиск связей по ИНН ---
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
                        baseName: null, // или какое-то значение, если есть
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
                        baseName: null,
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
                                    baseName: baseName, // Добавляем имя базы данных источника
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

async function findConnections(entities) {
    // --- НОРМАЛИЗАЦИЯ ВХОДНЫХ ДАННЫХ ---
    // Убедимся, что все сущности имеют нормализованные поля INN, UNID и т.д.
    // Убедимся, что все сущности имеют нормализованные поля INN, UNID и т.д.
    const normalizedEntities = entities.map(normalizeEntityForConnections);
    console.log("Нормализованные сущности для поиска связей:", normalizedEntities);

    // --- ИНТЕГРАЦИЯ СВЯЗЕЙ ЧЕРЕЗ МОДЕЛЬ ---
    console.log(`Найдено ${normalizedEntities.length} нормализованных сущностей для поиска связей.`);
    const connectionsMap = new Map(); // Для хранения всех связей

    // --- НОВЫЙ БЛОК: Поиск связей по email ---
    const entitiesWithMailAndKey = normalizedEntities.filter(entity => {
         const entityKey = entity.UNID || entity.fzUID || entity.cpUID || entity.PersonUNID;
         return entityKey && entity.eMail && entity.eMail.trim() !== '';
    });
    console.log(`Найдено ${entitiesWithMailAndKey.length} сущностей с ключом и email для поиска связей.`);
    // const emailConnectionsMap = await findConnectionsByEmail(entitiesWithMailAndKey);

    // --- НОВЫЙ БЛОК: Поиск связей по телефону ---
    const entitiesWithPhoneAndKey = normalizedEntities.filter(entity => {
         const entityKey = entity.UNID || entity.fzUID || entity.cpUID || entity.PersonUNID;
         const hasPhone = entity.PhoneNum || entity.fzPhoneM || entity.cpPhoneMob || entity.cpPhoneWork || entity.Phone;
         return entityKey && hasPhone;
    });
    console.log(`Найдено ${entitiesWithPhoneAndKey.length} сущностей с ключом и телефоном для поиска связей.`);
    const phoneConnectionsMap = await findConnectionsByPhone(entitiesWithPhoneAndKey);

    // --- НОВЫЙ БЛОК: Поиск связей по ИНН ---
    const entitiesWithINN = normalizedEntities.filter(entity => {
         const entityKey = entity.UNID || entity.fzUID || entity.cpUID || entity.PersonUNID; // Добавляем PersonUNID
         // Используем нормализованное поле INN
         return entityKey && entity.INN && entity.INN.trim() !== '';
    });
    console.log(`Найдено ${entitiesWithINN.length} сущностей с ключом и INN для поиска связей.`);
    console.log("Сущности с ИНН:", entitiesWithINN); // Лог для проверки
    const innConnectionsMap = await findConnectionsByINN(entitiesWithINN);

    // --- СОБИРАЕМ СВЯЗИ В ОДИН ОБЪЕКТ ---
    const allResultsWithConnections = normalizedEntities.map(item => {
        const entityKey = item.UNID || item.fzUID || item.cpUID || item.PersonUNID;
        let entityConnections = [];

        // Добавляем связи по email
        if (entityKey && entitiesWithMailAndKey.some(e => (e.UNID || e.fzUID || e.cpUID || e.PersonUNID) === entityKey)) {
            // Реализация для email, если нужна
        }

        // Добавляем связи по телефону
        if (entityKey && phoneConnectionsMap.has(entityKey)) {
            const phoneConnections = phoneConnectionsMap.get(entityKey) || {};
            for (const [phoneGroupKey, connections] of Object.entries(phoneConnections)) {
                entityConnections.push({
                    contact: phoneGroupKey,
                    type: 'contact',
                    subtype: 'phone',
                    connections: connections
                });
            }
        }

        // Добавляем связи по ИНН
        if (entityKey && innConnectionsMap.has(entityKey)) {
            const innConnections = innConnectionsMap.get(entityKey) || {};
            for (const [innGroupKey, connections] of Object.entries(innConnections)) {
                entityConnections.push({
                    contact: innGroupKey, // ИНН
                    type: 'inn',
                    subtype: 'inn_match',
                    connections: connections
                });
            }
        }

        // Возвращаем копию элемента с добавленным полем connections
        return {
            ...item,
            connections: entityConnections,
            connectionsCount: entityConnections.length // Добавляем счётчик связей
        };
    });

    // --- НОВЫЙ БЛОК: Объединение связей для юридических лиц из Delta с связями, найденными для их prevwork ---
    // Проходим по всем результатам
    for (const resultItem of allResultsWithConnections) {
        // Проверяем, является ли сущность юридическим лицом (например, по типу или наличию NameFull/NameShort и отсутствию PersonUNID как основного ключа)
        // Также проверяем, есть ли у неё INN
        if (resultItem.type === 'juridical' && resultItem.INN) { // Уточните условие, если тип может быть другим или не всегда доступен
             // Ищем в connectionsMap все связи, которые были найдены для PersonUNID, совпадающих с INN этой юр.лица
             // Для этого нужно найти все сущности prevwork с тем же INN
             const prevWorkEntitiesForThisINN = normalizedEntities.filter(e => e.type === 'prevwork' && e.INN === resultItem.INN && e.PersonUNID);
             // Извлекаем их PersonUNID
             const personUNIDsOfPrevWork = prevWorkEntitiesForThisINN.map(e => e.PersonUNID);

             // Теперь ищем в connectionsMap (которая была сформирована findConnectionsByINN) связи для этих PersonUNID
             for (const personUNID of personUNIDsOfPrevWork) {
                 if (innConnectionsMap.has(personUNID)) {
                     const prevWorkConnections = innConnectionsMap.get(personUNID) || {};
                     // Добавляем все группы связей из prevWork к юр.лицу
                     for (const [prevWorkINNKey, connections] of Object.entries(prevWorkConnections)) {
                         // Создаём или находим группу связей по ИНН для юрлица
                         let juridicalINNConnectionGroup = resultItem.connections.find(conn => conn.type === 'inn' && conn.subtype === 'inn_match' && conn.contact === prevWorkINNKey);
                         if (!juridicalINNConnectionGroup) {
                             juridicalINNConnectionGroup = {
                                 contact: prevWorkINNKey,
                                 type: 'inn',
                                 subtype: 'inn_match',
                                 connections: []
                             };
                             resultItem.connections.push(juridicalINNConnectionGroup);
                         }
                         // Добавляем связи из prevWork к группе юрлица
                         juridicalINNConnectionGroup.connections.push(...connections);
                     }
                 }
             }
        }
    }

    // Пересчитываем connectionsCount после добавления связей
    allResultsWithConnections.forEach(item => {
        item.connectionsCount = item.connections.length;
    });

    // --- НОВЫЙ БЛОК: Поиск связей для ИНН, найденных в результатах связей (Второй уровень) ---
    // (Остальная логика второго уровня, если нужна, но может потребовать адаптации под новую структуру)

    return allResultsWithConnections;
}

// --- ВСПОМОГАТЕЛЬНАЯ ФУНКЦИЯ: Нормализация сущности для поиска связей ---
// Применяет те же правила, что и в findLocalData
function normalizeEntityForConnections(entity) {
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

// ... (остальной код find_connections.js остается без изменений) ...

module.exports = {
    findConnections,
    findConnectionsByPhone,
    findConnectionsByINN, // Добавляем новую функцию
    determineEntityType
};