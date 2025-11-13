// queries.js
import { normalizePhoneSQL } from '../utils/helper.js';

export function buildContragentQuery() {
    return `
        SELECT
            UNID, ConCode, INN, KPP, OGRN, NameShort, NameFull, PhoneNum, eMail, UrFiz, fIP, AddressUr, AddressUFakt, fSZ, fIP as fIP_CG,
            BaseName
        FROM CI_Contragent_test
        WHERE
            (@query IS NOT NULL AND (INN LIKE @query OR NameShort LIKE @query OR NameFull LIKE @query OR UNID LIKE @query OR OGRN LIKE @query OR AddressUr LIKE @query OR AddressUFakt LIKE @query OR eMail LIKE @query))
            OR
            (@phoneQuery IS NOT NULL AND ${normalizePhoneSQL('PhoneNum')} = @phoneQuery)
            OR
            (@emailQuery IS NOT NULL AND eMail LIKE @emailQuery)
            OR
            (@ogrnQuery IS NOT NULL AND OGRN = @ogrnQuery)
    `;
}

export function buildEmployeeQuery() {
    return `
        SELECT
            fzUID, fzFIO, fzCode, fzDateB, fzAddress, fzAddressF, fzPhone, fzMail, fzINN,
            emUID, phOrgUID, phOrgINN, phOrgName, phDep, phFunction, phEventType, phContractType, phDate, phRegistrator, phRegUID,
            fzPhoneM,
            BaseName
        FROM CI_Employees_test
        WHERE
            (@query IS NOT NULL AND (fzFIO LIKE @query OR fzUID LIKE @query OR phOrgINN LIKE @query OR fzAddress LIKE @query OR fzAddressF LIKE @query OR fzMail LIKE @query))
            OR
            (@phoneQuery IS NOT NULL AND (${normalizePhoneSQL('fzPhone')} = @phoneQuery OR ${normalizePhoneSQL('fzPhoneM')} = @phoneQuery))
            OR
            (@emailQuery IS NOT NULL AND fzMail LIKE @emailQuery)
    `;
}

export function buildContPersonQuery() {
    return `
        SELECT
            cpUID, conUID, conCode, conINN, cpNameFull, cpName1, cpName2, cpName3, cpDateB, cpFunction, cpVid,
            cpPhoneMob, cpPhoneMobS, cpPhoneWork, cpMail, cpAddress, cpCountry, cpReg, cpTown,
            BaseName
        FROM CI_ContPersons_test
        WHERE
            (@query IS NOT NULL AND (cpNameFull LIKE @query OR cpUID LIKE @query OR conINN LIKE @query OR cpAddress LIKE @query OR cpTown LIKE @query OR cpMail LIKE @query))
            OR
            (@phoneQuery IS NOT NULL AND (${normalizePhoneSQL('cpPhoneMob')} = @phoneQuery OR ${normalizePhoneSQL('cpPhoneMobS')} = @phoneQuery OR ${normalizePhoneSQL('cpPhoneWork')} = @phoneQuery))
            OR
            (@emailQuery IS NOT NULL AND cpMail LIKE @emailQuery)
    `;
}

export function buildPrevWorkQuery() {
    return `
        SELECT
            PersonUNID, INN, OGRN, Caption, Phone, EMail, WorkPeriod
        FROM CF_PrevWork_test
        WHERE
            (@query IS NOT NULL AND (Caption LIKE @query OR PersonUNID LIKE @query OR INN LIKE @query OR EMail LIKE @query))
            OR
            (@phoneQuery IS NOT NULL AND ${normalizePhoneSQL('Phone')} = @phoneQuery)
            OR
            (@emailQuery IS NOT NULL AND EMail LIKE @emailQuery)
            OR
            (@ogrnQuery IS NOT NULL AND OGRN = @ogrnQuery)
    `;
}

export function buildPersonQuery() {
    return `
        SELECT
            UNID, INN, SNILS, FirstName, LastName, MiddleName, BirthDate, RegAddressPassport, RegAddressForm, ResAddressForm, State
        FROM CF_Persons_test
        WHERE
            (@query IS NOT NULL AND (LastName LIKE @query OR FirstName LIKE @query OR MiddleName LIKE @query OR UNID LIKE @query OR INN LIKE @query OR RegAddressPassport LIKE @query OR RegAddressForm LIKE @query OR ResAddressForm LIKE @query))
    `;
}

export function buildContactQuery() {
    return `
        SELECT
            PersonUNID, ContactType, Contact
        FROM CF_Contacts_test
        WHERE
            (@query IS NOT NULL AND (PersonUNID LIKE @query OR Contact LIKE @query))
            OR
            (@phoneQuery IS NOT NULL AND ${normalizePhoneSQL('Contact')} = @phoneQuery)
            OR
            (@emailQuery IS NOT NULL AND Contact LIKE @emailQuery)
    `;
}
