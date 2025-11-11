// api.js
// URL вашего сервера
const API_URL = 'http://localhost:3000/api/search';

// Функция для выполнения поиска
export async function performSearch(searchQuery) {
    if (searchQuery.length < 3) {
        alert('Введите не менее 3 символов для поиска.');
        return null;
    }

    try {
        const response = await fetch(API_URL, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ query: searchQuery })
        });

        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }

        const data = await response.json();
        console.log('Данные с сервера:', data); // Для отладки
        return data; // Возвращаем данные для дальнейшей обработки
    } catch (error) {
        console.error('Ошибка при поиске:', error);
        alert('Произошла ошибка при выполнении поиска. Проверьте консоль.');
        return null; // Возвращаем null при ошибке
    }
}