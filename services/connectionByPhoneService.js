import { cleanPhone, determineEntityType } from "../utils/helper.js";
import sql from 'mssql'


async function findConnectionsByPhone(targetEntities) {
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

        // console.log("Выполняемый SQL для телефона:", fullPhoneQuery); // Лог

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
                                // console.log(`Добавление связи по телефону: целевая ${targetEntityKey} -> найденная ${connectedEntityKey} (из ${row.sourceTable}), телефон ${matchingPhone}`); // Лог
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
                                // console.log(`Связь уже существует: целевая ${targetEntityKey} -> найденная ${connectedEntityKey}, телефон ${matchingPhone}`); // Лог
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

export {findConnectionsByPhone}