// src/database/queries/phone.queries.js

import { normalizePhoneSQL } from "../utils/helper.js";

// Вспомогательная функция для построения условия нормализации номера

function buildPhoneQuery(phoneArray) {
    if (phoneArray.length === 0) return 'SELECT 1 as dummy WHERE 1=0';

    // Нормализуем входящие номера в JavaScript перед передачей в SQL
    const normalizedPhoneParams = phoneArray.map(phone => {
        return phone.replace(/[^\d]/g, '');
    });

    const phoneParamNames = normalizedPhoneParams.map((_, index) => `@phone${index}`);

    // --- ПРЯМЫЕ ПОИСКИ ---
    // Кейс 1: CI_Contragent.PhoneNum
    const directContragentQuery = `
        SELECT
            ci.UNID as contactUNID,
            ci.INN as contactINN,
            ci.NameShort as contactNameShort,
            ci.NameFull as contactNameFull,
            ci.PhoneNum as contactPhone, -- Не нормализованный для отображения
            ci.UrFiz,
            ci.fIP,
            NULL as fzUID,
            NULL as cpUID,
            NULL as PersonUNID,
            'contragent' as sourceTable,
            ci.UNID as entityKey,
            ci.BaseName as baseName,
            NULL as relatedPersonUNID,
            NULL as prevWorkCaption
        FROM CI_Contragent_test ci
        WHERE ${normalizePhoneSQL('ci.PhoneNum')} IN (${phoneParamNames.join(', ')})
    `;

    // Кейс 2: CI_Employees.fzPhone, fzPhoneM
    // Используем CASE для возврата телефона, который совпал с условием WHERE
    const directEmployeeQuery = `
        SELECT
            NULL as contactUNID,
            ce.fzINN as contactINN, -- ИНН физлица сотрудника
            ce.fzFIO as contactNameShort,
            ce.fzFIO as contactNameFull,
            CASE
                WHEN ${normalizePhoneSQL('ce.fzPhone')} IN (${phoneParamNames.join(', ')}) THEN ce.fzPhone
                WHEN ${normalizePhoneSQL('ce.fzPhoneM')} IN (${phoneParamNames.join(', ')}) THEN ce.fzPhoneM
                ELSE NULL
            END as contactPhone, -- Возвращаем телефон, по которому был поиск
            2 as UrFiz, -- Физлицо
            0 as fIP,
            ce.fzUID as fzUID,
            NULL as cpUID,
            NULL as PersonUNID,
            'employee' as sourceTable,
            ce.fzUID as entityKey,
            ce.BaseName as baseName,
            NULL as relatedPersonUNID,
            NULL as prevWorkCaption
        FROM CI_Employees_test ce
        WHERE ${normalizePhoneSQL('ce.fzPhone')} IN (${phoneParamNames.join(', ')}) OR ${normalizePhoneSQL('ce.fzPhoneM')} IN (${phoneParamNames.join(', ')})
    `;

    // Кейс 3: CI_ContPersons.cpPhoneMob, cpPhoneWork, cpPhoneMobS
    // Используем CASE для возврата телефона, который совпал с условием WHERE
    const directContPersonQuery = `
        SELECT
            NULL as contactUNID,
            cip.conINN as contactINN, -- ИНН организации
            cip.cpNameFull as contactNameShort,
            cip.cpNameFull as contactNameFull,
            CASE
                WHEN ${normalizePhoneSQL('cip.cpPhoneMob')} IN (${phoneParamNames.join(', ')}) THEN cip.cpPhoneMob
                WHEN ${normalizePhoneSQL('cip.cpPhoneWork')} IN (${phoneParamNames.join(', ')}) THEN cip.cpPhoneWork
                WHEN ${normalizePhoneSQL('cip.cpPhoneMobS')} IN (${phoneParamNames.join(', ')}) THEN cip.cpPhoneMobS
                ELSE NULL
            END as contactPhone, -- Возвращаем телефон, по которому был поиск
            2 as UrFiz, -- Физлицо
            0 as fIP,
            NULL as fzUID,
            cip.cpUID as cpUID,
            NULL as PersonUNID,
            'contperson' as sourceTable,
            cip.cpUID as entityKey,
            cip.BaseName as baseName,
            NULL as relatedPersonUNID,
            NULL as prevWorkCaption
        FROM CI_ContPersons_test cip
        WHERE ${normalizePhoneSQL('cip.cpPhoneMob')} IN (${phoneParamNames.join(', ')}) OR ${normalizePhoneSQL('cip.cpPhoneWork')} IN (${phoneParamNames.join(', ')}) OR ${normalizePhoneSQL('cip.cpPhoneMobS')} IN (${phoneParamNames.join(', ')})
    `;

    // Кейс 7: CF_PrevWork.Phone -> CF_Persons (Прямой поиск по телефону в пред. работе, возврат человека)
    const directPrevWorkToPersonQuery = `
        SELECT
            p.UNID as contactUNID,
            p.INN as contactINN,
            CONCAT(p.LastName, ' ', p.FirstName, ' ', COALESCE(p.MiddleName, '')) as contactNameShort,
            CONCAT(p.LastName, ' ', p.FirstName, ' ', COALESCE(p.MiddleName, '')) as contactNameFull,
            cpw.Phone as contactPhone, -- Не нормализованный для отображения
            2 as UrFiz, -- Физлицо
            0 as fIP,
            NULL as fzUID,
            NULL as cpUID,
            p.UNID as PersonUNID,
            'person_from_prevwork_phone' as sourceTable, -- Новый тип источника
            p.UNID as entityKey,
            NULL as baseName,
            NULL as relatedPersonUNID,
            cpw.Caption as prevWorkCaption
        FROM CF_PrevWork_test cpw
        JOIN CF_Persons_test p ON cpw.PersonUNID = p.UNID
        WHERE ${normalizePhoneSQL('cpw.Phone')} IN (${phoneParamNames.join(', ')})
    `;

    // --- КОСВЕННЫЕ ПОИСКИ (через CF_Contacts) ---
    // Кейс 4 (точка входа) + 5, 6: CF_Contacts.Contact (телефон) -> другие таблицы
    // Предполагаем, что ContactType указывает на телефон или просто проверяем формат (цифры)
    const indirectContactToPersonQuery = `
        SELECT
            p.UNID as contactUNID,
            p.INN as contactINN,
            CONCAT(p.LastName, ' ', p.FirstName, ' ', COALESCE(p.MiddleName, '')) as contactNameShort,
            CONCAT(p.LastName, ' ', p.FirstName, ' ', COALESCE(p.MiddleName, '')) as contactNameFull,
            cc.Contact as contactPhone, -- Не нормализованный для отображения
            2 as UrFiz, -- Физлицо
            0 as fIP,
            NULL as fzUID,
            NULL as cpUID,
            p.UNID as PersonUNID,
            'person_via_contact' as sourceTable,
            p.UNID as entityKey,
            NULL as baseName,
            cc.PersonUNID as relatedPersonUNID,
            NULL as prevWorkCaption
        FROM CF_Contacts_test cc
        JOIN CF_Persons_test p ON cc.PersonUNID = p.UNID
        WHERE ${normalizePhoneSQL('cc.Contact')} IN (${phoneParamNames.join(', ')}) AND (LOWER(cc.ContactType) LIKE '%phone%' OR ${normalizePhoneSQL('cc.Contact')} LIKE '[0-9]%')
    `;

    const indirectContactToEmployeeQuery = `
        SELECT
            ce.fzINN as contactUNID, -- ИНН физлица сотрудника
            ce.fzINN as contactINN,
            ce.fzFIO as contactNameShort,
            ce.fzFIO as contactNameFull,
            cc.Contact as contactPhone,
            2 as UrFiz, -- Физлицо
            0 as fIP,
            ce.fzUID as fzUID,
            NULL as cpUID,
            ce.fzUID as PersonUNID,
            'employee_via_contact' as sourceTable,
            ce.fzUID as entityKey,
            ce.BaseName as baseName,
            cc.PersonUNID as relatedPersonUNID,
            NULL as prevWorkCaption
        FROM CF_Contacts_test cc
        JOIN CI_Employees_test ce ON cc.PersonUNID = ce.fzUID
        WHERE ${normalizePhoneSQL('cc.Contact')} IN (${phoneParamNames.join(', ')}) AND (LOWER(cc.ContactType) LIKE '%phone%' OR ${normalizePhoneSQL('cc.Contact')} LIKE '[0-9]%')
    `;

    const indirectContactToContPersonQuery = `
        SELECT
            cip.conINN as contactUNID, -- ИНН организации
            cip.conINN as contactINN,
            cip.cpNameFull as contactNameShort,
            cip.cpNameFull as contactNameFull,
            cc.Contact as contactPhone,
            2 as UrFiz, -- Физлицо
            0 as fIP,
            NULL as fzUID,
            cip.cpUID as cpUID,
            cip.cpUID as PersonUNID,
            'contperson_via_contact' as sourceTable,
            cip.cpUID as entityKey,
            cip.BaseName as baseName,
            cc.PersonUNID as relatedPersonUNID,
            NULL as prevWorkCaption
        FROM CF_Contacts_test cc
        JOIN CI_ContPersons_test cip ON cc.PersonUNID = cip.cpUID
        WHERE ${normalizePhoneSQL('cc.Contact')} IN (${phoneParamNames.join(', ')}) AND (LOWER(cc.ContactType) LIKE '%phone%' OR ${normalizePhoneSQL('cc.Contact')} LIKE '[0-9]%')
    `;

    // Кейс 4 (точка входа) + 7 (альтернативный путь): CF_Contacts.Contact (телефон) -> CF_PrevWork -> CF_Persons
    const indirectContactToPrevWorkToPersonQuery = `
        SELECT
            p.UNID as contactUNID,
            p.INN as contactINN,
            CONCAT(p.LastName, ' ', p.FirstName, ' ', COALESCE(p.MiddleName, '')) as contactNameShort,
            CONCAT(p.LastName, ' ', p.FirstName, ' ', COALESCE(p.MiddleName, '')) as contactNameFull,
            cc.Contact as contactPhone,
            2 as UrFiz, -- Физлицо
            0 as fIP,
            NULL as fzUID,
            NULL as cpUID,
            p.UNID as PersonUNID,
            'person_from_prevwork_via_contact' as sourceTable, -- Новый тип источника
            p.UNID as entityKey,
            NULL as baseName,
            cc.PersonUNID as relatedPersonUNID,
            cpw.Caption as prevWorkCaption
        FROM CF_Contacts_test cc
        JOIN CF_PrevWork_test cpw ON cc.PersonUNID = cpw.PersonUNID
        JOIN CF_Persons_test p ON cpw.PersonUNID = p.UNID
        WHERE ${normalizePhoneSQL('cc.Contact')} IN (${phoneParamNames.join(', ')}) AND (LOWER(cc.ContactType) LIKE '%phone%' OR ${normalizePhoneSQL('cc.Contact')} LIKE '[0-9]%')
    `;

    // Кейс 4 (сама сущность): CF_Contacts.Contact (если найден как телефон)
    const directContactQuery = `
        SELECT
            NULL as contactUNID,
            NULL as contactINN,
            cc.Contact as contactNameShort, -- Используем Contact как Name для самой записи
            cc.Contact as contactNameFull,
            cc.Contact as contactPhone,
            NULL as UrFiz,
            NULL as fIP,
            NULL as fzUID,
            NULL as cpUID,
            cc.PersonUNID,
            'contact' as sourceTable,
            cc.PersonUNID as entityKey,
            NULL as baseName,
            cc.PersonUNID as relatedPersonUNID,
            NULL as prevWorkCaption
        FROM CF_Contacts_test cc
        WHERE ${normalizePhoneSQL('cc.Contact')} IN (${phoneParamNames.join(', ')}) AND (LOWER(cc.ContactType) LIKE '%phone%' OR ${normalizePhoneSQL('cc.Contact')} LIKE '[0-9]%')
    `;

    // --- ОБЪЕДИНЕНИЕ ЗАПРОСОВ ---
    const directQueriesUnion = `(${directContragentQuery}) UNION ALL (${directEmployeeQuery}) UNION ALL (${directContPersonQuery}) UNION ALL (${directPrevWorkToPersonQuery})`;
    const indirectQueriesUnion = `(${indirectContactToPersonQuery}) UNION ALL (${indirectContactToEmployeeQuery}) UNION ALL (${indirectContactToContPersonQuery}) UNION ALL (${indirectContactToPrevWorkToPersonQuery}) UNION ALL (${directContactQuery})`;

    return `${directQueriesUnion} UNION ALL ${indirectQueriesUnion}`;
}

export { buildPhoneQuery, normalizePhoneSQL };