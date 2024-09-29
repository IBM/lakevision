// src/hooks.server.js

export async function handle({ event, resolve }) {
  console.log('Handle function called with event:', event);
  // Perform operations before the request is processed (e.g., setting CORS headers)
  const response = await resolve(event);

  // Add CORS headers globally
  response.headers.set('Access-Control-Allow-Origin', '*');
  response.headers.set('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  response.headers.set('Access-Control-Allow-Headers', 'Content-Type, Authorization');

  // Return the response with modified headers
  return response;
}

export async function handleError({ error, event }) {
  // Custom error handling logic
  console.error('An error occurred:', error);
  return {
    message: 'Something went wrong, please try again later.',
    code: error.code || 'UNKNOWN_ERROR'
  };
}
