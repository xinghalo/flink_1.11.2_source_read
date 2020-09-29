import { RendererFactory2 } from '@angular/core';
export declare class NzUpdateHostClassService {
    private classMap;
    private renderer;
    updateHostClass(el: HTMLElement, classMap: object): void;
    private removeClass;
    private addClass;
    constructor(rendererFactory2: RendererFactory2);
}
