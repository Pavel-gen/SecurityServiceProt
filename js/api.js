// js/api.js
async function search(query) {
    try {
        const res = await fetch('/api/search', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ query })
        });
        return await res.json();
    } catch (err) {
        console.error('Ошибка поиска:', err);
        return { juridical: [], physical: [], ip: [] };
    }
}