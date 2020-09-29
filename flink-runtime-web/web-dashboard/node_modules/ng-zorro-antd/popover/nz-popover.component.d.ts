import { ChangeDetectorRef, TemplateRef } from '@angular/core';
import { NzNoAnimationDirective } from '../core/no-animation/nz-no-animation.directive';
import { NzToolTipComponent } from '../tooltip/nz-tooltip.component';
export declare class NzPopoverComponent extends NzToolTipComponent {
    noAnimation?: NzNoAnimationDirective | undefined;
    _prefix: string;
    /** Used to remove NzToolTipComponent @ContentChild('nzTemplate') */
    nzTitle: string | TemplateRef<void>;
    nzContent: string | TemplateRef<void>;
    constructor(cdr: ChangeDetectorRef, noAnimation?: NzNoAnimationDirective | undefined);
    protected isContentEmpty(): boolean;
}
