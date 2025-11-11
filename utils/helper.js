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

export {determineEntityType, cleanPhone};