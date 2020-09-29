import { ChangeDetectorRef, ElementRef, OnDestroy, OnInit, Renderer2, TemplateRef } from '@angular/core';
import { NzNoAnimationDirective } from '../core/no-animation/nz-no-animation.directive';
import { NzOptionComponent } from './nz-option.component';
import { NzSelectService } from './nz-select.service';
export declare class NzSelectTopControlComponent implements OnInit, OnDestroy {
    private renderer;
    nzSelectService: NzSelectService;
    private cdr;
    noAnimation?: NzNoAnimationDirective | undefined;
    inputValue: string;
    isComposing: boolean;
    private destroy$;
    inputElement: ElementRef;
    nzShowSearch: boolean;
    nzPlaceHolder: string;
    nzOpen: boolean;
    nzMaxTagCount: number;
    nzAllowClear: boolean;
    nzShowArrow: boolean;
    nzLoading: boolean;
    nzSuffixIcon: TemplateRef<void>;
    nzClearIcon: TemplateRef<void>;
    nzRemoveIcon: TemplateRef<void>;
    nzMaxTagPlaceholder: TemplateRef<{
        $implicit: any[];
    }>;
    nzTokenSeparators: string[];
    onClearSelection(e: MouseEvent): void;
    setInputValue(value: string): void;
    readonly placeHolderDisplay: string;
    readonly selectedValueStyle: {
        [key: string]: string;
    };
    trackValue(_index: number, option: NzOptionComponent): any;
    updateWidth(): void;
    removeSelectedValue(option: NzOptionComponent, e: KeyboardEvent): void;
    constructor(renderer: Renderer2, nzSelectService: NzSelectService, cdr: ChangeDetectorRef, noAnimation?: NzNoAnimationDirective | undefined);
    ngOnInit(): void;
    ngOnDestroy(): void;
}
