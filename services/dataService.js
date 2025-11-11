import sql from 'mssql'
import 'dotenv/config';
import { config } from '../config/dbConfig.js';

// Приводит поля INN, UNID, fzUID, cpUID, PersonUNID к единому формату
function normalizeEntity(entity) {
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

async function fetchLocalData(query) {
    // Подключение к БД
    const pool = await sql.connect(config);

    // Подготовка параметров для SQL запросов
    const request = new sql.Request(pool);
    request.input('query', sql.VarChar, query);

    // --- СТАРЫЕ ЗАПРОСЫ ---
    // Запрос 1: Поиск в CI_Contragent_test
    const contragentQuery = `
        SELECT
            UNID, ConCode, INN, KPP, OGRN, NameShort, NameFull, PhoneNum, eMail, UrFiz, fIP, AddressUr, AddressUFakt, fSZ, fIP as fIP_CG,
            BaseName -- Добавляем BaseName
        FROM CI_Contragent_test
        WHERE INN LIKE @query OR NameShort LIKE @query OR NameFull LIKE @query OR UNID LIKE @query
    `;

    // Запрос 2: Поиск в CI_Employees_test
    const employeeQuery = `
        SELECT
            fzUID, fzFIO, fzCode, fzDateB, fzAddress, fzAddressF, fzPhone, fzMail, fzINN,
            emUID, phOrgUID, phOrgINN, phOrgName, phDep, phFunction, phEventType, phContractType, phDate, phRegistrator, phRegUID,
            fzPhoneM,
            BaseName -- Добавляем BaseName
        FROM CI_Employees_test
        WHERE fzFIO LIKE @query OR fzUID LIKE @query OR phOrgINN LIKE @query
    `;

    // Запрос 3: Поиск в CI_ContPersons_test
    const contPersonQuery = `
        SELECT
            cpUID, conUID, conCode, conINN, cpNameFull, cpName1, cpName2, cpName3, cpDateB, cpFunction, cpVid,
            cpPhoneMob, cpPhoneMobS, cpPhoneWork, cpMail, cpAddress, cpCountry, cpReg, cpTown,
            BaseName -- Добавляем BaseName
        FROM CI_ContPersons_test
        WHERE cpNameFull LIKE @query OR cpUID LIKE @query OR conINN LIKE @query
    `;

    // Запрос 4: Поиск в CF_PrevWork_test
    const prevWorkQuery = `
        SELECT
            PersonUNID, INN, OGRN, Caption, Phone, EMail, WorkPeriod
        FROM CF_PrevWork_test
        WHERE Caption LIKE @query OR PersonUNID LIKE @query OR INN LIKE @query
    `;

    // Запрос 5: Поиск в CF_Persons_test
    const personQuery = `
        SELECT
            UNID, INN, SNILS, FirstName, LastName, MiddleName, BirthDate, RegAddressPassport, RegAddressForm, ResAddressForm, State
        FROM CF_Persons_test
        WHERE LastName LIKE @query OR FirstName LIKE @query OR MiddleName LIKE @query OR UNID LIKE @query OR INN LIKE @query
    `;

    // Запрос 6: Поиск в CF_Contacts_test
    const contactQuery = `
        SELECT
            PersonUNID, ContactType, Contact
        FROM CF_Contacts_test
        WHERE Contact LIKE @query OR PersonUNID LIKE @query
    `;

    // Выполнение запросов параллельно
    const [contragentResult, employeeResult, contPersonResult, prevWorkResult, personResult, contactResult] = await Promise.all([
        request.query(contragentQuery),
        request.query(employeeQuery),
        request.query(contPersonQuery),
        request.query(prevWorkQuery),
        request.query(personQuery),
        request.query(contactQuery)
    ]);

    // --- НОРМАЛИЗАЦИЯ И ОБЪЕДИНЕНИЕ РЕЗУЛЬТАТОВ ---
    const contragents = contragentResult.recordset.map(row => {
        const normalized = normalizeEntity({
            UNID: row.UNID,
            ConCode: row.ConCode,
            INN: row.INN,
            KPP: row.KPP,
            OGRN: row.OGRN,
            NameShort: row.NameShort,
            NameFull: row.NameFull,
            PhoneNum: row.PhoneNum,
            eMail: row.eMail,
            UrFiz: row.UrFiz,
            fIP: row.fIP_CG, // Используем псевдоним
            AddressUr: row.AddressUr,
            AddressUFakt: row.AddressUFakt,
            fSZ: row.fSZ,
            type: 'juridical', // или 'ip' в зависимости от fIP
            // --- ДОБАВЛЯЕМ ИНФОРМАЦИЮ ОБ ИСТОЧНИКЕ ---
            source: 'local',
            sourceTable: 'CI_Contragent_test',
            baseName: row.BaseName
            // ---
        });
        // Дополнительная логика определения типа, если fIP не надёжно
        if (normalized.fIP === 1) normalized.type = 'ip';
        else if (normalized.UrFiz === 1) normalized.type = 'juridical';
        else if (normalized.UrFiz === 2) normalized.type = 'physical';
        return normalized;
    });

    const employees = employeeResult.recordset.map(row => normalizeEntity({
        fzUID: row.fzUID,
        fzFIO: row.fzFIO,
        fzCode: row.fzCode,
        fzDateB: row.fzDateB,
        fzAddress: row.fzAddress,
        fzAddressF: row.fzAddressF,
        fzPhone: row.fzPhone,
        fzMail: row.fzMail,
        fzINN: row.fzINN,
        emUID: row.emUID,
        phOrgUID: row.phOrgUID,
        phOrgINN: row.phOrgINN,
        phOrgName: row.phOrgName,
        phDep: row.phDep,
        phFunction: row.phFunction,
        phEventType: row.phEventType,
        phContractType: row.phContractType,
        phDate: row.phDate,
        phRegistrator: row.phRegistrator,
        phRegUID: row.phRegUID,
        fzPhoneM: row.fzPhoneM,
        type: 'physical',
        // --- ДОБАВЛЯЕМ ИНФОРМАЦИЮ ОБ ИСТОЧНИКЕ ---
        source: 'local',
        sourceTable: 'CI_Employees_test',
        baseName: row.BaseName
        // ---
    }));

    const contPersons = contPersonResult.recordset.map(row => normalizeEntity({
        cpUID: row.cpUID,
        conUID: row.conUID,
        conCode: row.conCode,
        conINN: row.conINN,
        cpNameFull: row.cpNameFull,
        cpName1: row.cpName1,
        cpName2: row.cpName2,
        cpName3: row.cpName3,
        cpDateB: row.cpDateB,
        cpFunction: row.cpFunction,
        cpVid: row.cpVid,
        cpPhoneMob: row.cpPhoneMob,
        cpPhoneMobS: row.cpPhoneMobS,
        cpPhoneWork: row.cpPhoneWork,
        cpMail: row.cpMail,
        cpAddress: row.cpAddress,
        cpCountry: row.cpCountry,
        cpReg: row.cpReg,
        cpTown: row.cpTown,
        type: 'physical',
        // --- ДОБАВЛЯЕМ ИНФОРМАЦИЮ ОБ ИСТОЧНИКЕ ---
        source: 'local',
        sourceTable: 'CI_ContPersons_test',
        baseName: row.BaseName
        // ---
    }));

    const prevWorks = prevWorkResult.recordset.map(row => normalizeEntity({
        PersonUNID: row.PersonUNID,
        INN: row.INN,
        OGRN: row.OGRN,
        Caption: row.Caption,
        Phone: row.Phone,
        eMail: row.EMail, // Обратите внимание на исходное имя столбца в запросе
        WorkPeriod: row.WorkPeriod,
        type: 'prevwork', // Указываем тип для удобства
        // --- ДОБАВЛЯЕМ ИНФОРМАЦИЮ ОБ ИСТОЧНИКЕ ---
        source: 'local',
        sourceTable: 'CF_PrevWork_test',
        baseName: null // CF_PrevWork_test не имеет BaseName
        // ---
    }));

    const persons = personResult.recordset.map(row => normalizeEntity({
        UNID: row.UNID,
        INN: row.INN,
        SNILS: row.SNILS,
        FirstName: row.FirstName,
        LastName: row.LastName,
        MiddleName: row.MiddleName,
        BirthDate: row.BirthDate,
        RegAddressPassport: row.RegAddressPassport,
        RegAddressForm: row.RegAddressForm,
        ResAddressForm: row.ResAddressForm,
        State: row.State,
        type: 'physical', // Указываем тип для удобства
        // --- ДОБАВЛЯЕМ ИНФОРМАЦИЮ ОБ ИСТОЧНИКЕ ---
        source: 'local',
        sourceTable: 'CF_Persons_test',
        baseName: null // CF_Persons_test не имеет BaseName
        // ---
    }));

    const contacts = contactResult.recordset.map(row => ({
        PersonUNID: row.PersonUNID,
        ContactType: row.ContactType,
        Contact: row.Contact,
        type: 'contact', // Указываем тип для удобства
        // --- ДОБАВЛЯЕМ ИНФОРМАЦИЮ ОБ ИСТОЧНИКЕ ---
        source: 'local',
        sourceTable: 'CF_Contacts_test',
        baseName: null // CF_Contacts_test не имеет BaseName
        // ---
    }));

    // Объединяем все результаты
    const allLocalResults = [...contragents, ...employees, ...contPersons, ...prevWorks, ...persons, ...contacts];

    // --- СОЗДАНИЕ СПИСКА ЦЕЛЕВЫХ СУЩНОСТЕЙ ДЛЯ ПОИСКА СВЯЗЕЙ ---
    // Фильтруем сущности, у которых есть ключ (UNID, fzUID, cpUID, PersonUNID) и ИНН
    const allTargetEntitiesForConnections = allLocalResults.filter(entity => {
        const entityKey = entity.UNID || entity.fzUID || entity.cpUID || entity.PersonUNID;
        // Используем нормализованное поле INN
        return entityKey && entity.INN && entity.INN.trim() !== '';
    });

    console.log(`Найдено ${allTargetEntitiesForConnections.length} локальных сущностей с ключом и INN для поиска связей.`);

    return {
        localResults: allLocalResults,
        allTargetEntitiesForConnections: allTargetEntitiesForConnections
    };
}

export { fetchLocalData };