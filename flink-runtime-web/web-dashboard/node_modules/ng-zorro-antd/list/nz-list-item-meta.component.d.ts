import { ElementRef, Renderer2, TemplateRef } from '@angular/core';
export declare class NzListItemMetaComponent {
    elementRef: ElementRef;
    private renderer;
    avatarStr: string;
    avatarTpl: TemplateRef<void>;
    nzAvatar: string | TemplateRef<void>;
    nzTitle: string | TemplateRef<void>;
    nzDescription: string | TemplateRef<void>;
    constructor(elementRef: ElementRef, renderer: Renderer2);
}
