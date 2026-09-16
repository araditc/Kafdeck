import { productDescription, productName } from '../shared/product.js';

export function AppShell() {
  return (
    <main aria-labelledby="kafdeck-title">
      <h1 id="kafdeck-title">{productName}</h1>
      <p>{productDescription}</p>
    </main>
  );
}
