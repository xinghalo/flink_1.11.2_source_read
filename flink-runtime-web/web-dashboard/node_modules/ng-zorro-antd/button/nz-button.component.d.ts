import { AfterContentInit, ChangeDetectorRef, ElementRef, NgZone, OnChanges, OnDestroy, OnInit, QueryList, Renderer2, SimpleChanges } from '@angular/core';
import { NzUpdateHostClassService } from '../core/services/update-host-class.service';
import { NzSizeLDSType } from '../core/types/size';
import { NzWaveConfig, NzWaveDirective } from '../core/wave/nz-wave.directive';
export declare type NzButtonType = 'primary' | 'dashed' | 'danger' | 'default';
export declare type NzButtonShape = 'circle' | 'round' | null;
export declare class NzButtonComponent implements AfterContentInit, OnInit, OnDestroy, OnChanges {
    private elementRef;
    private cdr;
    private renderer;
    private nzUpdateHostClassService;
    private ngZone;
    private waveConfig;
    private animationType;
    readonly el: HTMLElement;
    private iconElement;
    private iconOnly;
    contentElement: ElementRef;
    listOfIconElement: QueryList<ElementRef>;
    nzWave: NzWaveDirective;
    nzBlock: boolean;
    nzGhost: boolean;
    nzSearch: boolean;
    nzLoading: boolean;
    nzType: NzButtonType;
    nzShape: NzButtonShape;
    nzSize: NzSizeLDSType;
    /** temp solution since no method add classMap to host https://github.com/angular/angular/issues/7289 */
    setClassMap(): void;
    updateIconDisplay(value: boolean): void;
    checkContent(): void;
    moveIcon(): void;
    constructor(elementRef: ElementRef, cdr: ChangeDetectorRef, renderer: Renderer2, nzUpdateHostClassService: NzUpdateHostClassService, ngZone: NgZone, waveConfig: NzWaveConfig, animationType: string);
    ngAfterContentInit(): void;
    ngOnInit(): void;
    ngOnDestroy(): void;
    ngOnChanges(changes: SimpleChanges): void;
}
