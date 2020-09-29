import { ElementRef, Renderer2, TemplateRef } from '@angular/core';
export declare class NzCardMetaComponent {
    nzTitle: string | TemplateRef<void>;
    nzDescription: string | TemplateRef<void>;
    nzAvatar: TemplateRef<void>;
    constructor(elementRef: ElementRef, renderer: Renderer2);
}
