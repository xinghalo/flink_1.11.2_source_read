import { AfterContentInit, AfterViewInit, ChangeDetectorRef, ElementRef, OnDestroy, OnInit, Renderer2 } from '@angular/core';
import { FormControl, FormControlName } from '@angular/forms';
import { Subscription } from 'rxjs';
import { NzUpdateHostClassService } from '../core/services/update-host-class.service';
import { NgClassType } from '../core/types/ng-class';
import { NzColDirective } from '../grid/nz-col.directive';
import { NzRowDirective } from '../grid/nz-row.directive';
import { NzFormItemComponent } from './nz-form-item.component';
export declare class NzFormControlComponent extends NzColDirective implements OnDestroy, OnInit, AfterContentInit, AfterViewInit, OnDestroy {
    private cdr;
    private _hasFeedback;
    validateChanges: Subscription | null;
    validateString: string | null;
    controlClassMap: NgClassType;
    iconType: string;
    validateControl: FormControl | null;
    defaultValidateControl: FormControlName;
    nzHasFeedback: boolean;
    nzValidateStatus: string | FormControl | FormControlName;
    removeSubscribe(): void;
    watchControl(): void;
    validateControlStatus(status: string): boolean;
    setControlClassMap(): void;
    constructor(nzUpdateHostClassService: NzUpdateHostClassService, elementRef: ElementRef, nzFormItemComponent: NzFormItemComponent, nzRowDirective: NzRowDirective, cdr: ChangeDetectorRef, renderer: Renderer2);
    ngOnInit(): void;
    ngOnDestroy(): void;
    ngAfterContentInit(): void;
    ngAfterViewInit(): void;
}
