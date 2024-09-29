
/** @type {import('./$types').LayoutServerLoad} */
import { env } from '$env/dynamic/public';

export async function load({params, url}) {
    let res;
    const namespace = url.searchParams.get('namespace');
    if (namespace){
        res = await fetch(`${env.PUBLIC_API_SERVER}/tables?namespace=${namespace}`);
        if (res.ok) {
            const data = await res.json();
            console.log(data)
            return {"tables": data};
        }
    }
    else{
        res = await fetch(`${env.PUBLIC_API_SERVER}/namespaces`);
        if (res.ok) {
            const data = await res.json();            
            return {"namespaces": data};;
        }
    }
    console.log(res.ok)    
    
    const { message } = await res.json();
    return {
        error: new Error(message)
    };    
};
