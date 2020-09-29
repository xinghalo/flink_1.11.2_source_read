import { MediaMatcher } from '@angular/cdk/layout';
import { Platform } from '@angular/cdk/platform';
import { AfterContentInit, ChangeDetectorRef, ElementRef, NgZone, OnDestroy, QueryList, Renderer2 } from '@angular/core';
import { NzUpdateHostClassService } from '../core/services/update-host-class.service';
import { NzRowDirective } from '../grid/nz-row.directive';
import { NzFormExplainComponent } from './nz-form-explain.component';
/** should add nz-row directive to host, track https://github.com/angular/angular/issues/8785 **/
export declare class NzFormItemComponent extends NzRowDirective implements AfterContentInit, OnDestroy {
    private cdr;
    private _flex;
    listOfNzFormExplainComponent: QueryList<NzFormExplainComponent>;
    nzFlex: boolean;
    constructor(elementRef: ElementRef, renderer: Renderer2, nzUpdateHostClassService: NzUpdateHostClassService, mediaMatcher: MediaMatcher, ngZone: NgZone, platform: Platform, cdr: ChangeDetectorRef);
    ngAfterContentInit(): void;
}
