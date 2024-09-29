
/** @type {import('./$types').LayoutServerLoad} */
import { env } from '$env/dynamic/public';

export async function load({params, url}) {
    let res;   
    res = await fetch(`${env.PUBLIC_API_SERVER_SERVER_SIDE}/namespaces`);
    if (res.ok) {
        const data = await res.json();            
        return {"namespaces": data};;
    }    
    console.log(res.ok)        
    const { message } = await res.json();
    return {
        error: new Error(message)
    };    
};
