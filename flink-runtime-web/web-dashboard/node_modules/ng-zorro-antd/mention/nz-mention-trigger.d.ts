import { ElementRef, EventEmitter, ExistingProvider, OnDestroy } from '@angular/core';
import { ControlValueAccessor } from '@angular/forms';
import { Mention } from './nz-mention.component';
export declare const NZ_MENTION_TRIGGER_ACCESSOR: ExistingProvider;
export declare class NzMentionTriggerDirective implements ControlValueAccessor, OnDestroy {
    el: ElementRef;
    onChange: (value: string) => void;
    onTouched: () => void;
    readonly onFocusin: EventEmitter<void>;
    readonly onBlur: EventEmitter<void>;
    readonly onInput: EventEmitter<KeyboardEvent>;
    readonly onKeydown: EventEmitter<KeyboardEvent>;
    readonly onClick: EventEmitter<MouseEvent>;
    value: string;
    constructor(el: ElementRef);
    ngOnDestroy(): void;
    completeEvents(): void;
    focus(caretPos?: number): void;
    insertMention(mention: Mention): void;
    writeValue(value: string): void;
    registerOnChange(fn: (value: string) => void): void;
    registerOnTouched(fn: () => void): void;
}
