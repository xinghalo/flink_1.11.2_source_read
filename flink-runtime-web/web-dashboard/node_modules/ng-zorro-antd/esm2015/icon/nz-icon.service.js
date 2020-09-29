/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { DOCUMENT } from '@angular/common';
import { HttpBackend } from '@angular/common/http';
import { Inject, Injectable, InjectionToken, Optional, RendererFactory2 } from '@angular/core';
import { DomSanitizer } from '@angular/platform-browser';
import { IconService } from '@ant-design/icons-angular';
import { BarsOutline, CalendarOutline, CaretDownFill, CaretDownOutline, CaretUpFill, CaretUpOutline, CheckCircleFill, CheckCircleOutline, CheckOutline, ClockCircleOutline, CloseCircleFill, CloseCircleOutline, CloseOutline, DoubleLeftOutline, DoubleRightOutline, DownOutline, EllipsisOutline, ExclamationCircleFill, ExclamationCircleOutline, EyeOutline, FileFill, FileOutline, FilterFill, InfoCircleFill, InfoCircleOutline, LeftOutline, LoadingOutline, PaperClipOutline, QuestionCircleOutline, RightOutline, SearchOutline, StarFill, UploadOutline, UpOutline } from '@ant-design/icons-angular/icons';
import * as i0 from "@angular/core";
import * as i1 from "./public-api";
import * as i2 from "@angular/platform-browser";
import * as i3 from "@angular/common/http";
import * as i4 from "@angular/common";
/**
 * @record
 */
export function NzIconfontOption() { }
if (false) {
    /** @type {?} */
    NzIconfontOption.prototype.scriptUrl;
}
/** @type {?} */
export const NZ_ICONS = new InjectionToken('nz_icons');
/** @type {?} */
export const NZ_ICON_DEFAULT_TWOTONE_COLOR = new InjectionToken('nz_icon_default_twotone_color');
/** @type {?} */
export const DEFAULT_TWOTONE_COLOR = '#1890ff';
/** @type {?} */
export const NZ_ICONS_USED_BY_ZORRO = [
    BarsOutline,
    CalendarOutline,
    CaretUpFill,
    CaretUpOutline,
    CaretDownFill,
    CaretDownOutline,
    CheckCircleFill,
    CheckCircleOutline,
    CheckOutline,
    ClockCircleOutline,
    CloseCircleOutline,
    CloseCircleFill,
    CloseOutline,
    DoubleLeftOutline,
    DoubleRightOutline,
    DownOutline,
    EllipsisOutline,
    ExclamationCircleFill,
    ExclamationCircleOutline,
    EyeOutline,
    FileFill,
    FileOutline,
    FilterFill,
    InfoCircleFill,
    InfoCircleOutline,
    LeftOutline,
    LoadingOutline,
    PaperClipOutline,
    QuestionCircleOutline,
    RightOutline,
    StarFill,
    SearchOutline,
    StarFill,
    UploadOutline,
    UpOutline
];
/**
 * It should be a global singleton, otherwise registered icons could not be found.
 */
export class NzIconService extends IconService {
    /**
     * @param {?} rendererFactory
     * @param {?} sanitizer
     * @param {?} handler
     * @param {?} document
     * @param {?} icons
     * @param {?} defaultColor
     */
    constructor(rendererFactory, sanitizer, handler, document, icons, defaultColor) {
        super(rendererFactory, handler, document, sanitizer);
        this.rendererFactory = rendererFactory;
        this.sanitizer = sanitizer;
        this.handler = handler;
        this.document = document;
        this.icons = icons;
        this.defaultColor = defaultColor;
        this.iconfontCache = new Set();
        this.warnedAboutAPI = false;
        this.warnedAboutCross = false;
        this.warnedAboutVertical = false;
        this.addIcon(...NZ_ICONS_USED_BY_ZORRO, ...(this.icons || []));
        /** @type {?} */
        let primaryColor = DEFAULT_TWOTONE_COLOR;
        if (this.defaultColor) {
            if (this.defaultColor.startsWith('#')) {
                primaryColor = this.defaultColor;
            }
            else {
                console.warn('[NG-ZORRO]: twotone color must be a hex color!');
            }
        }
        this.twoToneColor = { primaryColor };
    }
    /**
     * @param {?} type
     * @return {?}
     */
    warnAPI(type) {
        if (type === 'old' && !this.warnedAboutAPI) {
            console.warn(`<i class="anticon"></i> would be deprecated soon. Please use <i nz-icon type=""></i> API.`);
            this.warnedAboutAPI = true;
        }
        if (type === 'cross' && !this.warnedAboutCross) {
            console.warn(`'cross' icon is replaced by 'close' icon.`);
            this.warnedAboutCross = true;
        }
        if (type === 'vertical' && !this.warnedAboutVertical) {
            console.warn(`'verticle' is misspelled, would be corrected in the next major version.`);
            this.warnedAboutVertical = true;
        }
    }
    /**
     * @param {?} svg
     * @return {?}
     */
    normalizeSvgElement(svg) {
        if (!svg.getAttribute('viewBox')) {
            this._renderer.setAttribute(svg, 'viewBox', '0 0 1024 1024');
        }
        if (!svg.getAttribute('width') || !svg.getAttribute('height')) {
            this._renderer.setAttribute(svg, 'width', '1em');
            this._renderer.setAttribute(svg, 'height', '1em');
        }
        if (!svg.getAttribute('fill')) {
            this._renderer.setAttribute(svg, 'fill', 'currentColor');
        }
    }
    /**
     * @param {?} opt
     * @return {?}
     */
    fetchFromIconfont(opt) {
        const { scriptUrl } = opt;
        if (this.document && !this.iconfontCache.has(scriptUrl)) {
            /** @type {?} */
            const script = this._renderer.createElement('script');
            this._renderer.setAttribute(script, 'src', scriptUrl);
            this._renderer.setAttribute(script, 'data-namespace', scriptUrl.replace(/^(https?|http):/g, ''));
            this._renderer.appendChild(this.document.body, script);
            this.iconfontCache.add(scriptUrl);
        }
    }
    /**
     * @param {?} type
     * @return {?}
     */
    createIconfontIcon(type) {
        return this._createSVGElementFromString(`<svg><use xlink:href="${type}"></svg>`);
    }
}
NzIconService.decorators = [
    { type: Injectable, args: [{
                providedIn: 'root'
            },] }
];
/** @nocollapse */
NzIconService.ctorParameters = () => [
    { type: RendererFactory2 },
    { type: DomSanitizer },
    { type: HttpBackend, decorators: [{ type: Optional }] },
    { type: undefined, decorators: [{ type: Optional }, { type: Inject, args: [DOCUMENT,] }] },
    { type: Array, decorators: [{ type: Optional }, { type: Inject, args: [NZ_ICONS,] }] },
    { type: String, decorators: [{ type: Optional }, { type: Inject, args: [NZ_ICON_DEFAULT_TWOTONE_COLOR,] }] }
];
/** @nocollapse */ NzIconService.ngInjectableDef = i0.defineInjectable({ factory: function NzIconService_Factory() { return new i1.NzIconService(i0.inject(i0.RendererFactory2), i0.inject(i2.DomSanitizer), i0.inject(i3.HttpBackend, 8), i0.inject(i4.DOCUMENT, 8), i0.inject(i1.NZ_ICONS, 8), i0.inject(i1.NZ_ICON_DEFAULT_TWOTONE_COLOR, 8)); }, token: i1.NzIconService, providedIn: "root" });
if (false) {
    /**
     * @type {?}
     * @private
     */
    NzIconService.prototype.iconfontCache;
    /**
     * @type {?}
     * @private
     */
    NzIconService.prototype.warnedAboutAPI;
    /**
     * @type {?}
     * @private
     */
    NzIconService.prototype.warnedAboutCross;
    /**
     * @type {?}
     * @private
     */
    NzIconService.prototype.warnedAboutVertical;
    /**
     * @type {?}
     * @protected
     */
    NzIconService.prototype.rendererFactory;
    /**
     * @type {?}
     * @protected
     */
    NzIconService.prototype.sanitizer;
    /**
     * @type {?}
     * @protected
     */
    NzIconService.prototype.handler;
    /**
     * @type {?}
     * @protected
     */
    NzIconService.prototype.document;
    /**
     * @type {?}
     * @private
     */
    NzIconService.prototype.icons;
    /**
     * @type {?}
     * @private
     */
    NzIconService.prototype.defaultColor;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotaWNvbi5zZXJ2aWNlLmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbImljb24vbnotaWNvbi5zZXJ2aWNlLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7QUFBQSxPQUFPLEVBQUUsUUFBUSxFQUFFLE1BQU0saUJBQWlCLENBQUM7QUFDM0MsT0FBTyxFQUFFLFdBQVcsRUFBRSxNQUFNLHNCQUFzQixDQUFDO0FBQ25ELE9BQU8sRUFBRSxNQUFNLEVBQUUsVUFBVSxFQUFFLGNBQWMsRUFBRSxRQUFRLEVBQUUsZ0JBQWdCLEVBQUUsTUFBTSxlQUFlLENBQUM7QUFDL0YsT0FBTyxFQUFFLFlBQVksRUFBRSxNQUFNLDJCQUEyQixDQUFDO0FBQ3pELE9BQU8sRUFBa0IsV0FBVyxFQUFFLE1BQU0sMkJBQTJCLENBQUM7QUFDeEUsT0FBTyxFQUNMLFdBQVcsRUFDWCxlQUFlLEVBQ2YsYUFBYSxFQUNiLGdCQUFnQixFQUNoQixXQUFXLEVBQ1gsY0FBYyxFQUNkLGVBQWUsRUFDZixrQkFBa0IsRUFDbEIsWUFBWSxFQUNaLGtCQUFrQixFQUNsQixlQUFlLEVBQ2Ysa0JBQWtCLEVBQ2xCLFlBQVksRUFDWixpQkFBaUIsRUFDakIsa0JBQWtCLEVBQ2xCLFdBQVcsRUFDWCxlQUFlLEVBQ2YscUJBQXFCLEVBQ3JCLHdCQUF3QixFQUN4QixVQUFVLEVBQ1YsUUFBUSxFQUNSLFdBQVcsRUFDWCxVQUFVLEVBQ1YsY0FBYyxFQUNkLGlCQUFpQixFQUNqQixXQUFXLEVBQ1gsY0FBYyxFQUNkLGdCQUFnQixFQUNoQixxQkFBcUIsRUFDckIsWUFBWSxFQUNaLGFBQWEsRUFDYixRQUFRLEVBQ1IsYUFBYSxFQUNiLFNBQVMsRUFDVixNQUFNLGlDQUFpQyxDQUFDOzs7Ozs7Ozs7QUFFekMsc0NBRUM7OztJQURDLHFDQUFrQjs7O0FBR3BCLE1BQU0sT0FBTyxRQUFRLEdBQUcsSUFBSSxjQUFjLENBQUMsVUFBVSxDQUFDOztBQUN0RCxNQUFN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QyxTQUFTLENBQUMsV0FBVyxDQUFDLElBQUksQ0FBQyxRQUFRLENBQUMsSUFBSSxFQUFFLE1BQU0sQ0FBQyxDQUFDO1lBQ3ZELElBQUksQ0FBQyxhQUFhLENBQUMsR0FBRyxDQUFDLFNBQVMsQ0FBQyxDQUFDO1NBQ25DO0lBQ0gsQ0FBQzs7Ozs7SUFFRCxrQkFBa0IsQ0FBQyxJQUFZO1FBQzdCLE9BQU8sSUFBSSxDQUFDLDJCQUEyQixDQUFDLHlCQUF5QixJQUFJLFVBQVUsQ0FBQyxDQUFDO0lBQ25GLENBQUM7OztZQWxERixVQUFVLFNBQUM7Z0JBQ1YsVUFBVSxFQUFFLE1BQU07YUFDbkI7Ozs7WUExRnNELGdCQUFnQjtZQUM5RCxZQUFZO1lBRlosV0FBVyx1QkFnSmYsUUFBUTs0Q0FFUixRQUFRLFlBQUksTUFBTSxTQUFDLFFBQVE7d0NBQzNCLFFBQVEsWUFBSSxNQUFNLFNBQUMsUUFBUTt5Q0FDM0IsUUFBUSxZQUFJLE1BQU0sU0FBQyw2QkFBNkI7Ozs7Ozs7O0lBdkRuRCxzQ0FBMEM7Ozs7O0lBQzFDLHVDQUErQjs7Ozs7SUFDL0IseUNBQWlDOzs7OztJQUNqQyw0Q0FBb0M7Ozs7O0lBOENsQyx3Q0FBMkM7Ozs7O0lBQzNDLGtDQUFpQzs7Ozs7SUFDakMsZ0NBQTBDOzs7OztJQUUxQyxpQ0FBcUQ7Ozs7O0lBQ3JELDhCQUE2RDs7Ozs7SUFDN0QscUNBQStFIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHsgRE9DVU1FTlQgfSBmcm9tICdAYW5ndWxhci9jb21tb24nO1xuaW1wb3J0IHsgSHR0cEJhY2tlbmQgfSBmcm9tICdAYW5ndWxhci9jb21tb24vaHR0cCc7XG5pbXBvcnQgeyBJbmplY3QsIEluamVjdGFibGUsIEluamVjdGlvblRva2VuLCBPcHRpb25hbCwgUmVuZGVyZXJGYWN0b3J5MiB9IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuaW1wb3J0IHsgRG9tU2FuaXRpemVyIH0gZnJvbSAnQGFuZ3VsYXIvcGxhdGZvcm0tYnJvd3Nlcic7XG5pbXBvcnQgeyBJY29uRGVmaW5pdGlvbiwgSWNvblNlcnZpY2UgfSBmcm9tICdAYW50LWRlc2lnbi9pY29ucy1hbmd1bGFyJztcbmltcG9ydCB7XG4gIEJhcnNPdXRsaW5lLFxuICBDYWxlbmRhck91dGxpbmUsXG4gIENhcmV0RG93bkZpbGwsXG4gIENhcmV0RG93bk91dGxpbmUsXG4gIENhcmV0VXBGaWxsLFxuICBDYXJldFVwT3V0bGluZSxcbiAgQ2hlY2tDaXJjbGVGaWxsLFxuICBDaGVja0NpcmNsZU91dGxpbmUsXG4gIENoZWNrT3V0bGluZSxcbiAgQ2xvY2tDaXJjbGVPdXRsaW5lLFxuICBDbG9zZUNpcmNsZUZpbGwsXG4gIENsb3NlQ2lyY2xlT3V0bGluZSxcbiAgQ2xvc2VPdXRsaW5lLFxuICBEb3VibGVMZWZ0T3V0bGluZSxcbiAgRG91YmxlUmlnaHRPdXRsaW5lLFxuICBEb3duT3V0bGluZSxcbiAgRWxsaXBzaXNPdXRsaW5lLFxuICBFeGNsYW1hdGlvbkNpcmNsZUZpbGwsXG4gIEV4Y2xhbWF0aW9uQ2lyY2xlT3V0bGluZSxcbiAgRXllT3V0bGluZSxcbiAgRmlsZUZpbGwsXG4gIEZpbGVPdXRsaW5lLFxuICBGaWx0ZXJGaWxsLFxuICBJbmZvQ2lyY2xlRmlsbCxcbiAgSW5mb0NpcmNsZU91dGxpbmUsXG4gIExlZnRPdXRsaW5lLFxuICBMb2FkaW5nT3V0bGluZSxcbiAgUGFwZXJDbGlwT3V0bGluZSxcbiAgUXVlc3Rpb25DaXJjbGVPdXRsaW5lLFxuICBSaWdodE91dGxpbmUsXG4gIFNlYXJjaE91dGxpbmUsXG4gIFN0YXJGaWxsLFxuICBVcGxvYWRPdXRsaW5lLFxuICBVcE91dGxpbmVcbn0gZnJvbSAnQGFudC1kZXNpZ24vaWNvbnMtYW5ndWxhci9pY29ucyc7XG5cbmV4cG9ydCBpbnRlcmZhY2UgTnpJY29uZm9udE9wdGlvbiB7XG4gIHNjcmlwdFVybDogc3RyaW5nO1xufVxuXG5leHBvcnQgY29uc3QgTlpfSUNPTlMgPSBuZXcgSW5qZWN0aW9uVG9rZW4oJ256X2ljb25zJyk7XG5leHBvcnQgY29uc3QgTlpfSUNPTl9ERUZBVUxUX1RXT1RPTkVfQ09MT1IgPSBuZXcgSW5qZWN0aW9uVG9rZW4oJ256X2ljb25fZGVmYXVsdF90d290b25lX2NvbG9yJyk7XG5leHBvcnQgY29uc3QgREVGQVVMVF9UV09UT05FX0NPTE9SID0gJyMxODkwZmYnO1xuZXhwb3J0IGNvbnN0IE5aX0lDT05TX1VTRURfQllfWk9SUk86IEljb25EZWZpbml0aW9uW10gPSBbXG4gIEJhcnNPdXRsaW5lLFxuICBDYWxlbmRhck91dGxpbmUsXG4gIENhcmV0VXBGaWxsLFxuICBDYXJldFVwT3V0bGluZSxcbiAgQ2FyZXREb3duRmlsbCxcbiAgQ2FyZXREb3duT3V0bGluZSxcbiAgQ2hlY2tDaXJjbGVGaWxsLFxuICBDaGVja0NpcmNsZU91dGxpbmUsXG4gIENoZWNrT3V0bGluZSxcbiAgQ2xvY2tDaXJjbGVPdXRsaW5lLFxuICBDbG9zZUNpcmNsZU91dGxpbmUsXG4gIENsb3NlQ2lyY2xlRmlsbCxcbiAgQ2xvc2VPdXRsaW5lLFxuICBEb3VibGVMZWZ0T3V0bGluZSxcbiAgRG91YmxlUmlnaHRPdXRsaW5lLFxuICBEb3duT3V0bGluZSxcbiAgRWxsaXBzaXNPdXRsaW5lLFxuICBFeGNsYW1hdGlvbkNpcmNsZUZpbGwsXG4gIEV4Y2xhbWF0aW9uQ2lyY2xlT3V0bGluZSxcbiAgRXllT3V0bGluZSxcbiAgRmlsZUZpbGwsXG4gIEZpbGVPdXRsaW5lLFxuICBGaWx0ZXJGaWxsLFxuICBJbmZvQ2lyY2xlRmlsbCxcbiAgSW5mb0NpcmNsZU91dGxpbmUsXG4gIExlZnRPdXRsaW5lLFxuICBMb2FkaW5nT3V0bGluZSxcbiAgUGFwZXJDbGlwT3V0bGluZSxcbiAgUXVlc3Rpb25DaXJjbGVPdXRsaW5lLFxuICBSaWdodE91dGxpbmUsXG4gIFN0YXJGaWxsLFxuICBTZWFyY2hPdXRsaW5lLFxuICBTdGFyRmlsbCxcbiAgVXBsb2FkT3V0bGluZSxcbiAgVXBPdXRsaW5lXG5dO1xuXG4vKipcbiAqIEl0IHNob3VsZCBiZSBhIGdsb2JhbCBzaW5nbGV0b24sIG90aGVyd2lzZSByZWdpc3RlcmVkIGljb25zIGNvdWxkIG5vdCBiZSBmb3VuZC5cbiAqL1xuQEluamVjdGFibGUoe1xuICBwcm92aWRlZEluOiAncm9vdCdcbn0pXG5leHBvcnQgY2xhc3MgTnpJY29uU2VydmljZSBleHRlbmRzIEljb25TZXJ2aWNlIHtcbiAgcHJpdmF0ZSBpY29uZm9udENhY2hlID0gbmV3IFNldDxzdHJpbmc+KCk7XG4gIHByaXZhdGUgd2FybmVkQWJvdXRBUEkgPSBmYWxzZTtcbiAgcHJpdmF0ZSB3YXJuZWRBYm91dENyb3NzID0gZmFsc2U7XG4gIHByaXZhdGUgd2FybmVkQWJvdXRWZXJ0aWNhbCA9IGZhbHNlO1xuXG4gIHdhcm5BUEkodHlwZTogJ29sZCcgfCAnY3Jvc3MnIHwgJ3ZlcnRpY2FsJyk6IHZvaWQge1xuICAgIGlmICh0eXBlID09PSAnb2xkJyAmJiAhdGhpcy53YXJuZWRBYm91dEFQSSkge1xuICAgICAgY29uc29sZS53YXJuKGA8aSBjbGFzcz1cImFudGljb25cIj48L2k+IHdvdWxkIGJlIGRlcHJlY2F0ZWQgc29vbi4gUGxlYXNlIHVzZSA8aSBuei1pY29uIHR5cGU9XCJcIj48L2k+IEFQSS5gKTtcbiAgICAgIHRoaXMud2FybmVkQWJvdXRBUEkgPSB0cnVlO1xuICAgIH1cbiAgICBpZiAodHlwZSA9PT0gJ2Nyb3NzJyAmJiAhdGhpcy53YXJuZWRBYm91dENyb3NzKSB7XG4gICAgICBjb25zb2xlLndhcm4oYCdjcm9zcycgaWNvbiBpcyByZXBsYWNlZCBieSAnY2xvc2UnIGljb24uYCk7XG4gICAgICB0aGlzLndhcm5lZEFib3V0Q3Jvc3MgPSB0cnVlO1xuICAgIH1cbiAgICBpZiAodHlwZSA9PT0gJ3ZlcnRpY2FsJyAmJiAhdGhpcy53YXJuZWRBYm91dFZlcnRpY2FsKSB7XG4gICAgICBjb25zb2xlLndhcm4oYCd2ZXJ0aWNsZScgaXMgbWlzc3BlbGxlZCwgd291bGQgYmUgY29ycmVjdGVkIGluIHRoZSBuZXh0IG1ham9yIHZlcnNpb24uYCk7XG4gICAgICB0aGlzLndhcm5lZEFib3V0VmVydGljYWwgPSB0cnVlO1xuICAgIH1cbiAgfVxuXG4gIG5vcm1hbGl6ZVN2Z0VsZW1lbnQoc3ZnOiBTVkdFbGVtZW50KTogdm9pZCB7XG4gICAgaWYgKCFzdmcuZ2V0QXR0cmlidXRlKCd2aWV3Qm94JykpIHtcbiAgICAgIHRoaXMuX3JlbmRlcmVyLnNldEF0dHJpYnV0ZShzdmcsICd2aWV3Qm94JywgJzAgMCAxMDI0IDEwMjQnKTtcbiAgICB9XG4gICAgaWYgKCFzdmcuZ2V0QXR0cmlidXRlKCd3aWR0aCcpIHx8ICFzdmcuZ2V0QXR0cmlidXRlKCdoZWlnaHQnKSkge1xuICAgICAgdGhpcy5fcmVuZGVyZXIuc2V0QXR0cmlidXRlKHN2ZywgJ3dpZHRoJywgJzFlbScpO1xuICAgICAgdGhpcy5fcmVuZGVyZXIuc2V0QXR0cmlidXRlKHN2ZywgJ2hlaWdodCcsICcxZW0nKTtcbiAgICB9XG4gICAgaWYgKCFzdmcuZ2V0QXR0cmlidXRlKCdmaWxsJykpIHtcbiAgICAgIHRoaXMuX3JlbmRlcmVyLnNldEF0dHJpYnV0ZShzdmcsICdmaWxsJywgJ2N1cnJlbnRDb2xvcicpO1xuICAgIH1cbiAgfVxuXG4gIGZldGNoRnJvbUljb25mb250KG9wdDogTnpJY29uZm9udE9wdGlvbik6IHZvaWQge1xuICAgIGNvbnN0IHsgc2NyaXB0VXJsIH0gPSBvcHQ7XG4gICAgaWYgKHRoaXMuZG9jdW1lbnQgJiYgIXRoaXMuaWNvbmZvbnRDYWNoZS5oYXMoc2NyaXB0VXJsKSkge1xuICAgICAgY29uc3Qgc2NyaXB0ID0gdGhpcy5fcmVuZGVyZXIuY3JlYXRlRWxlbWVudCgnc2NyaXB0Jyk7XG4gICAgICB0aGlzLl9yZW5kZXJlci5zZXRBdHRyaWJ1dGUoc2NyaXB0LCAnc3JjJywgc2NyaXB0VXJsKTtcbiAgICAgIHRoaXMuX3JlbmRlcmVyLnNldEF0dHJpYnV0ZShzY3JpcHQsICdkYXRhLW5hbWVzcGFjZScsIHNjcmlwdFVybC5yZXBsYWNlKC9eKGh0dHBzP3xodHRwKTovZywgJycpKTtcbiAgICAgIHRoaXMuX3JlbmRlcmVyLmFwcGVuZENoaWxkKHRoaXMuZG9jdW1lbnQuYm9keSwgc2NyaXB0KTtcbiAgICAgIHRoaXMuaWNvbmZvbnRDYWNoZS5hZGQoc2NyaXB0VXJsKTtcbiAgICB9XG4gIH1cblxuICBjcmVhdGVJY29uZm9udEljb24odHlwZTogc3RyaW5nKTogU1ZHRWxlbWVudCB7XG4gICAgcmV0dXJuIHRoaXMuX2NyZWF0ZVNWR0VsZW1lbnRGcm9tU3RyaW5nKGA8c3ZnPjx1c2UgeGxpbms6aHJlZj1cIiR7dHlwZX1cIj48L3N2Zz5gKTtcbiAgfVxuXG4gIGNvbnN0cnVjdG9yKFxuICAgIHByb3RlY3RlZCByZW5kZXJlckZhY3Rvcnk6IFJlbmRlcmVyRmFjdG9yeTIsXG4gICAgcHJvdGVjdGVkIHNhbml0aXplcjogRG9tU2FuaXRpemVyLFxuICAgIEBPcHRpb25hbCgpIHByb3RlY3RlZCBoYW5kbGVyOiBIdHRwQmFja2VuZCxcbiAgICAvLyB0c2xpbnQ6ZGlzYWJsZS1uZXh0LWxpbmU6bm8tYW55XG4gICAgQE9wdGlvbmFsKCkgQEluamVjdChET0NVTUVOVCkgcHJvdGVjdGVkIGRvY3VtZW50OiBhbnksXG4gICAgQE9wdGlvbmFsKCkgQEluamVjdChOWl9JQ09OUykgcHJpdmF0ZSBpY29uczogSWNvbkRlZmluaXRpb25bXSxcbiAgICBAT3B0aW9uYWwoKSBASW5qZWN0KE5aX0lDT05fREVGQVVMVF9UV09UT05FX0NPTE9SKSBwcml2YXRlIGRlZmF1bHRDb2xvcjogc3RyaW5nXG4gICkge1xuICAgIHN1cGVyKHJlbmRlcmVyRmFjdG9yeSwgaGFuZGxlciwgZG9jdW1lbnQsIHNhbml0aXplcik7XG5cbiAgICB0aGlzLmFkZEljb24oLi4uTlpfSUNPTlNfVVNFRF9CWV9aT1JSTywgLi4uKHRoaXMuaWNvbnMgfHwgW10pKTtcblxuICAgIGxldCBwcmltYXJ5Q29sb3IgPSBERUZBVUxUX1RXT1RPTkVfQ09MT1I7XG4gICAgaWYgKHRoaXMuZGVmYXVsdENvbG9yKSB7XG4gICAgICBpZiAodGhpcy5kZWZhdWx0Q29sb3Iuc3RhcnRzV2l0aCgnIycpKSB7XG4gICAgICAgIHByaW1hcnlDb2xvciA9IHRoaXMuZGVmYXVsdENvbG9yO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgY29uc29sZS53YXJuKCdbTkctWk9SUk9dOiB0d290b25lIGNvbG9yIG11c3QgYmUgYSBoZXggY29sb3IhJyk7XG4gICAgICB9XG4gICAgfVxuICAgIHRoaXMudHdvVG9uZUNvbG9yID0geyBwcmltYXJ5Q29sb3IgfTtcbiAgfVxufVxuIl19