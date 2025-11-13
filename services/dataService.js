// fetchLocalData.js
import sql from 'mssql';
import 'dotenv/config';
import { config } from '../config/dbConfig.js';
import { cleanPhone, determineQueryType, getEntityKey } from '../utils/helper.js'; // Обновите путь
import { normalizePhoneSQL } from '../utils/helper.js'; // Импортируем, если нужна в других местах
import {
    buildContragentQuery,
    buildEmployeeQuery,
    buildContPersonQuery,
    buildPrevWorkQuery,
    buildPersonQuery,
    buildContactQuery
} from '../queries/firstBD.queries.js'; // Обновите путь

// Импортируем normalizeEntity, если он был в fetchLocalData
function normalizeEntity(entity) {
    // ... ваша логика normalizeEntity ...
    if (entity && entity.inn && !entity.INN) {
        entity.INN = entity.inn;
    }
    if (entity && entity.ogrn && !entity.OGRN) {
        entity.OGRN = entity.ogrn;
    }
    if (entity && entity.name_short && !entity.NameShort) {
        entity.NameShort = entity.name_short;
    }
    if (entity && entity.name_full && !entity.NameFull) {
        entity.NameFull = entity.name_full;
    }
    if (entity && entity.email && !entity.eMail) {
        entity.eMail = entity.email;
    }
    if (entity && entity.phone && !entity.PhoneNum) {
        entity.PhoneNum = entity.phone;
    }
    if (entity && entity.address_ur && !entity.AddressUr) {
        entity.AddressUr = entity.address_ur;
    }
    if (entity && entity.address_ufakt && !entity.AddressUFakt) {
        entity.AddressUFakt = entity.address_ufakt;
    }
    if (entity && entity.ur_fiz && !entity.UrFiz) {
        entity.UrFiz = entity.ur_fiz;
    }
    if (entity && entity.f_ip !== undefined && entity.fIP === undefined) {
        entity.fIP = entity.f_ip;
    }
    if (entity && entity.fIP !== undefined) {
        entity.fIP = Boolean(entity.fIP);
    }
    if (entity && entity.UrFiz !== undefined) {
        entity.UrFiz = Number(entity.UrFiz);
    }
    return entity;
}

async function fetchLocalData(query) {
    console.log(`Получен запрос: ${query}`);
    const queryInfo = determineQueryType(query);
    console.log(`Тип запроса: ${queryInfo.type}, Значение для поиска: ${queryInfo.value}`);

    const pool = await sql.connect(config);
    const request = new sql.Request(pool);

    let queryParam = queryInfo.value;
    let phoneParam = null;
    let emailParam = null;
    let textParam = null;
    let ogrnParam = null;

    if (queryInfo.type === 'phone') {
        phoneParam = queryInfo.value;
        textParam = `%${query}%`;
    } else if (queryInfo.type === 'email') {
        emailParam = `%${queryInfo.value}%`;
        textParam = `%${query}%`;
    } else if (queryInfo.type === 'ogrn') {
        ogrnParam = queryInfo.value;
        textParam = `%${query}%`;
    } else {
        textParam = `%${query}%`;
    }

    request.input('query', sql.VarChar, textParam);
    request.input('phoneQuery', sql.VarChar, phoneParam);
    request.input('emailQuery', sql.VarChar, emailParam);
    request.input('ogrnQuery', sql.VarChar, ogrnParam);

    const [
        contragentResult,
        employeeResult,
        contPersonResult,
        prevWorkResult,
        personResult,
        contactResult
    ] = await Promise.all([
        request.query(buildContragentQuery()),
        request.query(buildEmployeeQuery()),
        request.query(buildContPersonQuery()),
        request.query(buildPrevWorkQuery()),
        request.query(buildPersonQuery()),
        request.query(buildContactQuery())
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
            fIP: row.fIP_CG,
            AddressUr: row.AddressUr,
            AddressUFakt: row.AddressUFakt,
            fSZ: row.fSZ,
            type: 'juridical',
            source: 'local',
            sourceTable: 'CI_Contragent_test',
            baseName: row.BaseName
        });

        if (normalized.fIP === 1 || normalized.fIP === true) {
             normalized.type = 'ip';
        } else if (normalized.UrFiz === 1) {
             normalized.type = 'juridical';
        } else if (normalized.UrFiz === 2) {
             normalized.type = 'physical';
        }
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
        source: 'local',
        sourceTable: 'CI_Employees_test',
        baseName: row.BaseName,
        NameShort: row.fzFIO,
        NameFull: row.fzFIO
    }));

    const contPersons = contPersonResult.recordset.map(row => {
        const firstName = row.cpName2 || '';
        const lastName = row.cpName1 || '';
        const middleName = row.cpName3 || '';
        const fullName = `${lastName} ${firstName} ${middleName}`.trim();

        return normalizeEntity({
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
            NameShort: fullName || row.cpNameFull,
            NameFull: fullName || row.cpNameFull,
            type: 'physical',
            source: 'local',
            sourceTable: 'CI_ContPersons_test',
            baseName: row.BaseName
        });
    });

    const prevWorks = prevWorkResult.recordset.map(row => normalizeEntity({
        PersonUNID: row.PersonUNID,
        INN: row.INN,
        OGRN: row.OGRN,
        Caption: row.Caption,
        Phone: row.Phone,
        eMail: row.EMail,
        WorkPeriod: row.WorkPeriod,
        type: 'prevwork',
        source: 'local',
        sourceTable: 'CF_PrevWork_test',
        baseName: null
    }));

    const persons = personResult.recordset.map(row => {
        const firstName = row.FirstName || '';
        const lastName = row.LastName || '';
        const middleName = row.MiddleName || '';
        const fullName = `${lastName} ${firstName} ${middleName}`.trim();

        const entity = normalizeEntity({
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
            NameShort: fullName || row.INN,
            NameFull: fullName || row.INN,
            type: 'physical',
            source: 'local',
            sourceTable: 'CF_Persons_test',
            baseName: null
        });

        return entity;
    });

    const contacts = contactResult.recordset.map(row => ({
        PersonUNID: row.PersonUNID,
        ContactType: row.ContactType,
        Contact: row.Contact,
        type: 'contact',
        source: 'local',
        sourceTable: 'CF_Contacts_test',
        baseName: null
    }));

    const allLocalResults = [...contragents, ...employees, ...contPersons, ...prevWorks, ...persons, ...contacts];

    // --- СОЗДАНИЕ СПИСКА ЦЕЛЕВЫХ СУЩНОСТЕЙ ДЛЯ ПОИСКА СВЯЗЕЙ ---
    // Используем новую getEntityKey для уникальности
    const allTargetEntitiesForConnections = allLocalResults.filter(entity => {
        const entityKey = getEntityKey(entity); // Теперь использует уникальный ключ
        return entityKey && entity.INN && entity.INN.trim() !== '';
    });

    console.log(`Найдено ${allTargetEntitiesForConnections.length} локальных сущностей с ключом и INN для поиска связей.`);
    console.log(`Найдено ${allLocalResults.length} локальных сущностей по параметрам (ИНН, ФИО, UNID, Caption, Телефон, Email, Адрес, OGRN).`);

    return {
        localResults: allLocalResults,
        allTargetEntitiesForConnections: allTargetEntitiesForConnections
    };
}

export { fetchLocalData };
