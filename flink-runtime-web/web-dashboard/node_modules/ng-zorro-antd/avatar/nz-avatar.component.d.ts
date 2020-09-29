import { ChangeDetectorRef, ElementRef, OnChanges, Renderer2, SimpleChanges } from '@angular/core';
import { NzUpdateHostClassService } from '../core/services/update-host-class.service';
import { NzSizeLDSType } from '../core/types/size';
export declare type NzAvatarShape = 'square' | 'circle';
export declare type NzAvatarSize = NzSizeLDSType | number;
export interface NzAvatarSizeMap {
    [size: string]: string;
}
export declare class NzAvatarComponent implements OnChanges {
    private elementRef;
    private cd;
    private updateHostClassService;
    private renderer;
    nzShape: NzAvatarShape;
    nzSize: NzAvatarSize;
    nzText: string;
    nzSrc: string;
    nzIcon: string;
    oldAPIIcon: boolean;
    hasText: boolean;
    hasSrc: boolean;
    hasIcon: boolean;
    textStyles: {};
    textEl: ElementRef;
    private el;
    private prefixCls;
    private sizeMap;
    constructor(elementRef: ElementRef, cd: ChangeDetectorRef, updateHostClassService: NzUpdateHostClassService, renderer: Renderer2);
    setClass(): this;
    imgError(): void;
    ngOnChanges(changes: SimpleChanges): void;
    private calcStringSize;
    private notifyCalc;
    private setSizeStyle;
}
