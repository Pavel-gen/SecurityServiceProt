function buildEmailQuery(emailArray) {
    if (emailArray.length === 0) return 'SELECT 1 as dummy WHERE 1=0';

    const emailParams = emailArray.map((_, index) => `@email${index}`);

    // --- ПРЯМЫЕ ПОИСКИ ---
    // Кейс 1: CI_Contragent.eMail
    const directContragentQuery = `
        SELECT
            ci.UNID as contactUNID,
            ci.INN as contactINN,
            ci.NameShort as contactNameShort,
            ci.NameFull as contactNameFull,
            ci.eMail as contactEmail,
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
        WHERE LOWER(ci.eMail) IN (${emailParams.join(', ')})
    `;

    // Кейс 2: CI_ContPersons.cpMail
    const directContPersonQuery = `
        SELECT
            NULL as contactUNID,
            cip.conINN as contactINN,
            cip.cpNameFull as contactNameShort,
            cip.cpNameFull as contactNameFull,
            cip.cpMail as contactEmail,
            2 as UrFiz,
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
        WHERE LOWER(cip.cpMail) IN (${emailParams.join(', ')})
    `;

    // Кейс 3: CI_Employees.fzMail
    const directEmployeeQuery = `
        SELECT
            NULL as contactUNID,
            ce.fzINN as contactINN,
            ce.fzFIO as contactNameShort,
            ce.fzFIO as contactNameFull,
            ce.fzMail as contactEmail,
            2 as UrFiz,
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
        WHERE LOWER(ce.fzMail) IN (${emailParams.join(', ')})
    `;

    // Кейс 8: CF_PrevWork.EMail -> CF_Persons (Прямой поиск по email в пред. работе, возврат человека)
    const directPrevWorkToPersonQuery = `
        SELECT
            p.UNID as contactUNID,
            p.INN as contactINN,
            CONCAT(p.LastName, ' ', p.FirstName, ' ', COALESCE(p.MiddleName, '')) as contactNameShort,
            CONCAT(p.LastName, ' ', p.FirstName, ' ', COALESCE(p.MiddleName, '')) as contactNameFull,
            cpw.EMail as contactEmail,
            2 as UrFiz,
            0 as fIP,
            NULL as fzUID,
            NULL as cpUID,
            p.UNID as PersonUNID,
            'person_from_prevwork_email' as sourceTable,
            p.UNID as entityKey,
            NULL as baseName,
            NULL as relatedPersonUNID,
            cpw.Caption as prevWorkCaption
        FROM CF_PrevWork_test cpw
        JOIN CF_Persons_test p ON cpw.PersonUNID = p.UNID
        WHERE LOWER(cpw.EMail) IN (${emailParams.join(', ')})
    `;

    // --- КОСВЕННЫЕ ПОИСКИ (через CF_Contacts) ---
    // Кейс 4 (точка входа) + 5: CF_Contacts.Contact -> CF_Persons
    const indirectContactToPersonQuery = `
        SELECT
            p.UNID as contactUNID,
            p.INN as contactINN,
            CONCAT(p.LastName, ' ', p.FirstName, ' ', COALESCE(p.MiddleName, '')) as contactNameShort,
            CONCAT(p.LastName, ' ', p.FirstName, ' ', COALESCE(p.MiddleName, '')) as contactNameFull,
            cc.Contact as contactEmail,
            2 as UrFiz,
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
        WHERE LOWER(cc.Contact) IN (${emailParams.join(', ')}) 
    `;

    // Кейс 4 (точка входа) + 6: CF_Contacts.Contact -> CI_Employees
    const indirectContactToEmployeeQuery = `
        SELECT
            ce.fzINN as contactUNID,
            ce.fzINN as contactINN,
            ce.fzFIO as contactNameShort,
            ce.fzFIO as contactNameFull,
            cc.Contact as contactEmail,
            2 as UrFiz,
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
        WHERE LOWER(cc.Contact) IN (${emailParams.join(', ')}) 
    `;

    // Кейс 4 (точка входа) + 7: CF_Contacts.Contact -> CI_ContPersons
    const indirectContactToContPersonQuery = `
        SELECT
            cip.conINN as contactUNID,
            cip.conINN as contactINN,
            cip.cpNameFull as contactNameShort,
            cip.cpNameFull as contactNameFull,
            cc.Contact as contactEmail,
            2 as UrFiz,
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
        WHERE LOWER(cc.Contact) IN (${emailParams.join(', ')}) 
    `;

    // Кейс 4 (точка входа) + 8 (альтернативный путь): CF_Contacts.Contact -> CF_PrevWork -> CF_Persons
    const indirectContactToPrevWorkToPersonQuery = `
        SELECT
            p.UNID as contactUNID,
            p.INN as contactINN,
            CONCAT(p.LastName, ' ', p.FirstName, ' ', COALESCE(p.MiddleName, '')) as contactNameShort,
            CONCAT(p.LastName, ' ', p.FirstName, ' ', COALESCE(p.MiddleName, '')) as contactNameFull,
            cc.Contact as contactEmail,
            2 as UrFiz,
            0 as fIP,
            NULL as fzUID,
            NULL as cpUID,
            p.UNID as PersonUNID,
            'person_from_prevwork_via_contact' as sourceTable,
            p.UNID as entityKey,
            NULL as baseName,
            cc.PersonUNID as relatedPersonUNID,
            cpw.Caption as prevWorkCaption
        FROM CF_Contacts_test cc
        JOIN CF_PrevWork_test cpw ON cc.PersonUNID = cpw.PersonUNID
        JOIN CF_Persons_test p ON cpw.PersonUNID = p.UNID
        WHERE LOWER(cc.Contact) IN (${emailParams.join(', ')}) 
    `;

    // Кейс 4 (сама сущность): CF_Contacts.Contact (если найдена как email)
    const directContactQuery = `
        SELECT
            NULL as contactUNID,
            NULL as contactINN,
            cc.Contact as contactNameShort, -- Используем Contact как Name для самой записи
            cc.Contact as contactNameFull,
            cc.Contact as contactEmail,
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
        WHERE LOWER(cc.Contact) IN (${emailParams.join(', ')}) 
    `;

    // --- ОБЪЕДИНЕНИЕ ЗАПРОСОВ ---
    // Сначала объединим прямые
    const directQueriesUnion = `(${directContragentQuery}) UNION ALL (${directContPersonQuery}) UNION ALL (${directEmployeeQuery}) UNION ALL (${directPrevWorkToPersonQuery})`;
    // Затем объединим косвенные
    const indirectQueriesUnion = `(${indirectContactToPersonQuery}) UNION ALL (${indirectContactToEmployeeQuery}) UNION ALL (${indirectContactToContPersonQuery}) UNION ALL (${indirectContactToPrevWorkToPersonQuery}) UNION ALL (${directContactQuery})`;

    // Объединим все
    return `${directQueriesUnion} UNION ALL ${indirectQueriesUnion}`;
}

export {buildEmailQuery}