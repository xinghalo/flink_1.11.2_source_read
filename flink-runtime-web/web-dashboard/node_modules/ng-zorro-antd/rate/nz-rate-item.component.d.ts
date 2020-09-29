import { EventEmitter, TemplateRef } from '@angular/core';
export declare class NzRateItemComponent {
    character: TemplateRef<void>;
    allowHalf: boolean;
    readonly itemHover: EventEmitter<boolean>;
    readonly itemClick: EventEmitter<boolean>;
    hoverRate(isHalf: boolean): void;
    clickRate(isHalf: boolean): void;
}
