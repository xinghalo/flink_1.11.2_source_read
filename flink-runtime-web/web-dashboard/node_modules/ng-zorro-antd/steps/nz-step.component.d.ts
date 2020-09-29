import { ChangeDetectorRef, ElementRef, Renderer2, TemplateRef } from '@angular/core';
import { NgClassType } from '../core/types/ng-class';
export declare class NzStepComponent {
    private cdr;
    processDotTemplate: TemplateRef<void>;
    nzTitle: string | TemplateRef<void>;
    nzDescription: string | TemplateRef<void>;
    nzStatus: string;
    isCustomStatus: boolean;
    private _status;
    nzIcon: NgClassType | TemplateRef<void>;
    oldAPIIcon: boolean;
    isIconString: boolean;
    private _icon;
    customProcessTemplate: TemplateRef<{
        $implicit: TemplateRef<void>;
        status: string;
        index: number;
    }>;
    direction: string;
    index: number;
    last: boolean;
    outStatus: string;
    showProcessDot: boolean;
    currentIndex: number;
    private _currentIndex;
    constructor(cdr: ChangeDetectorRef, renderer: Renderer2, elementRef: ElementRef);
    markForCheck(): void;
}
