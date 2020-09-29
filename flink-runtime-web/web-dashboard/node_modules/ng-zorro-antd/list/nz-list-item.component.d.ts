import { ElementRef, QueryList, Renderer2, TemplateRef } from '@angular/core';
import { NzListItemMetaComponent } from './nz-list-item-meta.component';
export declare class NzListItemComponent {
    elementRef: ElementRef;
    private renderer;
    metas: QueryList<NzListItemMetaComponent>;
    nzActions: Array<TemplateRef<void>>;
    nzContent: string | TemplateRef<void>;
    nzExtra: TemplateRef<void>;
    constructor(elementRef: ElementRef, renderer: Renderer2);
}
