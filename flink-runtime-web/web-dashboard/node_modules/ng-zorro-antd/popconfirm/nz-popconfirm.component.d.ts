import { ChangeDetectorRef, EventEmitter, TemplateRef } from '@angular/core';
import { NzNoAnimationDirective } from '../core/no-animation/nz-no-animation.directive';
import { NzToolTipComponent } from '../tooltip/nz-tooltip.component';
export declare class NzPopconfirmComponent extends NzToolTipComponent {
    noAnimation?: NzNoAnimationDirective | undefined;
    _prefix: string;
    _trigger: string;
    _hasBackdrop: boolean;
    nzOkText: string;
    nzOkType: string;
    nzCancelText: string;
    nzCondition: boolean;
    nzIcon: string | TemplateRef<void>;
    readonly nzOnCancel: EventEmitter<void>;
    readonly nzOnConfirm: EventEmitter<void>;
    constructor(cdr: ChangeDetectorRef, noAnimation?: NzNoAnimationDirective | undefined);
    show(): void;
    onCancel(): void;
    onConfirm(): void;
}
