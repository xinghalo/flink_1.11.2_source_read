import { ElementRef, Renderer2, TemplateRef } from '@angular/core';
import { NzCardTabComponent } from './nz-card-tab.component';
export declare class NzCardComponent {
    nzBordered: boolean;
    nzLoading: boolean;
    nzHoverable: boolean;
    nzBodyStyle: {
        [key: string]: string;
    };
    nzCover: TemplateRef<void>;
    nzActions: Array<TemplateRef<void>>;
    nzType: string;
    nzTitle: string | TemplateRef<void>;
    nzExtra: string | TemplateRef<void>;
    tab: NzCardTabComponent;
    constructor(renderer: Renderer2, elementRef: ElementRef);
}
