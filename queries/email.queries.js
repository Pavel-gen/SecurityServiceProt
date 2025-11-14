// queries/email.queries.js

function buildEmailQuery(emailArray) {
    if (emailArray.length === 0) return 'SELECT 1 as dummy WHERE 1=0';

    const emailParams = emailArray.map((_, index) => `@email${index}`);

    // --- ПРЯМЫЕ ПОИСКИ ---
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
            'CI_Contragent_test' as sourceTable, -- ЕДИНООБРАЗНОЕ НАЗВАНИЕ
            ci.UNID as entityKey,
            ci.BaseName as baseName,
            NULL as relatedPersonUNID,
            NULL as prevWorkCaption
        FROM CI_Contragent_test ci
        WHERE LOWER(ci.eMail) IN (${emailParams.join(', ')})
    `;

    const directContPersonQuery = `
        SELECT
            NULL as contactUNID,
            cip.conINN as contactINN,
            cip.cpNameFull as contactNameFull,
            cip.cpNameFull as NameFull,
            cip.cpMail as contactEmail,
            2 as UrFiz,
            0 as fIP,
            NULL as fzUID,
            cip.cpUID as cpUID,
            NULL as PersonUNID,
            'CI_ContPersons_test' as sourceTable, -- ЕДИНООБРАЗНОЕ НАЗВАНИЕ
            cip.cpUID as entityKey,
            cip.BaseName as baseName,
            NULL as relatedPersonUNID,
            NULL as prevWorkCaption
        FROM CI_ContPersons_test cip
        WHERE LOWER(cip.cpMail) IN (${emailParams.join(', ')})
    `;

    const directEmployeeQuery = `
        SELECT
            ce.fzUID as contactUNID,
            ce.fzINN as contactINN,
            ce.fzFIO as contactNameShort,
            ce.fzFIO as contactNameFull,
            ce.fzMail as contactEmail,
            2 as UrFiz,
            0 as fIP,
            ce.fzUID as fzUID,
            NULL as cpUID,
            ce.fzUID as fzUID,
            'CI_Employees_test' as sourceTable, -- ЕДИНООБРАЗНОЕ НАЗВАНИЕ
            ce.fzUID as entityKey,
            ce.BaseName as baseName,
            NULL as relatedPersonUNID,
            NULL as prevWorkCaption
        FROM CI_Employees_test ce
        WHERE LOWER(ce.fzMail) IN (${emailParams.join(', ')})
    `;

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
            'CF_PrevWork_test' as sourceTable, -- ЕДИНООБРАЗНОЕ НАЗВАНИЕ
            p.UNID as entityKey,
            NULL as baseName,
            NULL as relatedPersonUNID,
            cpw.Caption as prevWorkCaption
        FROM CF_PrevWork_test cpw
        JOIN CF_Persons_test p ON cpw.PersonUNID = p.UNID
        WHERE LOWER(cpw.EMail) IN (${emailParams.join(', ')})
    `;

    // --- КОСВЕННЫЕ ПОИСКИ ---
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
            'CI_Employees_test' as sourceTable, -- ЕДИНООБРАЗНОЕ НАЗВАНИЕ
            ce.fzUID as entityKey,
            ce.BaseName as baseName,
            cc.PersonUNID as relatedPersonUNID,
            NULL as prevWorkCaption
        FROM CF_Contacts_test cc
        JOIN CI_Employees_test ce ON cc.PersonUNID = ce.fzUID
        WHERE LOWER(cc.Contact) IN (${emailParams.join(', ')})
    `;

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
            'CI_ContPersons_test' as sourceTable, -- ЕДИНООБРАЗНОЕ НАЗВАНИЕ
            cip.cpUID as entityKey,
            cip.BaseName as baseName,
            cc.PersonUNID as relatedPersonUNID,
            NULL as prevWorkCaption
        FROM CF_Contacts_test cc
        JOIN CI_ContPersons_test cip ON cc.PersonUNID = cip.cpUID
        WHERE LOWER(cc.Contact) IN (${emailParams.join(', ')})
    `;

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
            'CF_Persons_test' as sourceTable, -- ЕДИНООБРАЗНОЕ НАЗВАНИЕ
            p.UNID as entityKey,
            NULL as baseName,
            cc.PersonUNID as relatedPersonUNID,
            cpw.Caption as prevWorkCaption
        FROM CF_Contacts_test cc
        JOIN CF_PrevWork_test cpw ON cc.PersonUNID = cpw.PersonUNID
        JOIN CF_Persons_test p ON cpw.PersonUNID = p.UNID
        WHERE LOWER(cc.Contact) IN (${emailParams.join(', ')})
    `;

    const directContactQuery = `
        SELECT
            NULL as contactUNID,
            NULL as contactINN,
            cc.Contact as contactNameShort,
            cc.Contact as contactNameFull,
            cc.Contact as contactEmail,
            NULL as UrFiz,
            NULL as fIP,
            NULL as fzUID,
            NULL as cpUID,
            cc.PersonUNID,
            'CF_Contacts_test' as sourceTable, -- ЕДИНООБРАЗНОЕ НАЗВАНИЕ
            cc.PersonUNID as entityKey,
            NULL as baseName,
            cc.PersonUNID as relatedPersonUNID,
            NULL as prevWorkCaption
        FROM CF_Contacts_test cc
        WHERE LOWER(cc.Contact) IN (${emailParams.join(', ')})
    `;

    // --- ОБЪЕДИНЕНИЕ ЗАПРОСОВ ---
    return `
        (${directContragentQuery}) 
        UNION ALL (${directContPersonQuery}) 
        UNION ALL (${directEmployeeQuery}) 
        UNION ALL (${directPrevWorkToPersonQuery})
        UNION ALL (${indirectContactToEmployeeQuery}) 
        UNION ALL (${indirectContactToContPersonQuery}) 
        UNION ALL (${indirectContactToPrevWorkToPersonQuery}) 
        UNION ALL (${directContactQuery})
    `;
}

export { buildEmailQuery };