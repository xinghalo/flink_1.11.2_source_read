import { AfterContentChecked, ElementRef, OnChanges, OnDestroy, OnInit, Renderer2, SimpleChanges } from '@angular/core';
import { IconDirective, ThemeType } from '@ant-design/icons-angular';
import { NzIconService } from './nz-icon.service';
/**
 * This directive extends IconDirective to provide:
 *
 * - IconFont support
 * - spinning
 * - old API compatibility
 *
 * @break-changes
 *
 * - old API compatibility, icon class names would not be supported.
 * - properties that not started with `nz`.
 */
export declare class NzIconDirective extends IconDirective implements OnInit, OnChanges, OnDestroy, AfterContentChecked {
    iconService: NzIconService;
    elementRef: ElementRef;
    renderer: Renderer2;
    /** Properties with `nz` prefix. */
    nzSpin: boolean;
    nzRotate: number;
    nzType: string;
    nzTheme: ThemeType;
    nzTwotoneColor: string;
    nzIconfont: string;
    /** @deprecated 8.0.0 avoid exposing low layer API. */
    spin: boolean;
    /** @deprecated 8.0.0 avoid exposing low layer API. */
    iconfont: string;
    type: string;
    private classNameObserver;
    private el;
    private _type;
    /**
     * Replacement of `changeIcon` for more modifications.
     * @param oldAPI
     */
    private changeIcon2;
    private classChangeHandler;
    private handleSpin;
    private handleRotate;
    private setClassName;
    private setSVGData;
    constructor(iconService: NzIconService, elementRef: ElementRef, renderer: Renderer2);
    ngOnChanges(changes: SimpleChanges): void;
    ngOnInit(): void;
    ngOnDestroy(): void;
    /**
     * If custom content is provided, try to normalize SVG elements.
     */
    ngAfterContentChecked(): void;
}
