/** keep track https://github.com/angular/material2/issues/5007 **/
import { Overlay } from '@angular/cdk/overlay';
import { TemplateRef } from '@angular/core';
import { NzDropdownContextComponent } from './nz-dropdown-context.component';
export declare class NzDropdownService {
    private overlay;
    private overlayRef;
    constructor(overlay: Overlay);
    create($event: MouseEvent, templateRef: TemplateRef<void>): NzDropdownContextComponent;
    dispose(): void;
}
